package sidecarterminator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

const (
	SidecarTerminatorContainerNamePrefix = "sidecar-terminator"
)

// SidecarTerminator defines an instance of the sidecar terminator.
type SidecarTerminator struct {
	clientset       *kubernetes.Clientset // The k8s client.
	terminatorImage string                // The image to use for terminating the sidecars.
	sidecars        map[string]int        // A map of sidecar names to process signal interrupt numbers.
	namespaces      []string              // A list namespaces to monitor. Empty will monitor all namespaces.
}

// NewSidecarTerminator returns a new SidecarTerminator instance.
func NewSidecarTerminator(config *rest.Config, clientset *kubernetes.Clientset, terminatorImage string, sidecarsstr, namespaces []string) (*SidecarTerminator, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	if clientset == nil {
		return nil, errors.New("clientset cannot be nil")
	}

	if terminatorImage == "" {
		return nil, errors.New("sidecarTerminatorImage cannot be empty")
	}

	sidecars := map[string]int{}
	for _, sidecar := range sidecarsstr {
		comp := strings.Split(sidecar, "=")
		if len(comp) == 1 {
			sidecars[comp[0]] = int(syscall.SIGTERM)
		} else if len(comp) == 2 {
			num, err := strconv.Atoi(comp[1])
			if err != nil {
				return nil, err
			}

			sidecars[comp[0]] = num
		} else {
			return nil, fmt.Errorf("incorrect sidecar container name format: %s", sidecar)
		}
	}

	return &SidecarTerminator{
		clientset:       clientset,
		terminatorImage: terminatorImage,
		sidecars:        sidecars,
		namespaces:      namespaces,
	}, nil
}

// Run runs the sidecar terminator.
// TODO: Ensure this is only called once.
func (st *SidecarTerminator) Run(ctx context.Context) error {
	klog.Info("starting sidecar terminator")

	// Setup shared informer factory
	if len(st.namespaces) == 0 {
		if err := st.setupInformerForNamespace(ctx, metav1.NamespaceAll); err != nil {
			return err
		}
	} else {
		for _, namespace := range st.namespaces {
			if err := st.setupInformerForNamespace(ctx, namespace); err != nil {
				return err
			}
		}
	}

	<-ctx.Done()
	klog.Info("terminating sidecar terminator")
	return nil
}

func (st *SidecarTerminator) setupInformerForNamespace(ctx context.Context, namespace string) error {
	if namespace == v1.NamespaceAll {
		klog.Info("starting shared informer")
	} else {
		klog.Infof("starting shared informer for namespace %q", namespace)
	}

	factory := informers.NewFilteredSharedInformerFactory(
		st.clientset,
		time.Minute*10,
		namespace,
		nil,
	)

	factory.Core().V1().Pods().Informer().AddEventHandler(
		&sidecarTerminatorEventHandler{
			st: st,
		})
	factory.Start(ctx.Done())
	for _, ok := range factory.WaitForCacheSync(nil) {
		if !ok {
			return errors.New("timed out waiting for controller caches to sync")
		}
	}

	return nil
}

func (st *SidecarTerminator) terminate(pod *v1.Pod) error {
	klog.Infof("Found running sidecar containers in %s", podName(pod))

	// Terminate the sidecar
	for _, sidecar := range pod.Status.ContainerStatuses {
		if isSidecarContainer(sidecar.Name, st.sidecars) && sidecar.State.Running != nil && !hasSidecarTerminatorContainer(pod, sidecar) {

			// TODO: Add ability to kill the proper process
			// May require looking into the OCI image to extract the entrypoint if not
			// available via the containers' command.
			if pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace {
				klog.Error("Containers are sharing process namespace: ending process 1 will not end sidecars.")
				return fmt.Errorf("unable to end sidecar %s in pod %s using shareProcessNamespace", sidecar.Name, podName(pod))
			}

			klog.Infof("Terminating sidecar %s from %s with signal %d", sidecar.Name, podName(pod), st.sidecars[sidecar.Name])

			securityContext, err := getSidecarSecurityContext(pod, sidecar.Name)
			if err != nil {
				klog.Errorf(err.Error())
				return err
			}

			pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, v1.EphemeralContainer{
				TargetContainerName: sidecar.Name,
				EphemeralContainerCommon: v1.EphemeralContainerCommon{
					Name:  generateSidecarTerminatorContainerName(sidecar.Name),
					Image: st.terminatorImage,
					Command: []string{
						"kill",
					},
					Args: []string{
						fmt.Sprintf("-%d", st.sidecars[sidecar.Name]),
						"1",
					},
					ImagePullPolicy: v1.PullAlways,
					SecurityContext: securityContext,
				},
			})

			_, err = st.clientset.CoreV1().Pods(pod.Namespace).UpdateEphemeralContainers(context.TODO(), pod.Name, pod, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
