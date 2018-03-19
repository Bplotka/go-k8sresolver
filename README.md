
# NOTE

This project was moved and incorporated by [kedge](https://github.com/improbable-eng/kedge) project and is maintained here: https://github.com/improbable-eng/kedge/tree/master/pkg/resolvers/k8s
Please feel free to add issue to this project if you would like to have standalone library instead for this useful feature.

# k8sresolver

Kubernetes resolver based on [endpoint API](https://kubernetes.io/docs/api-reference/v1.7/#endpoints-v1-core)

Inspired by https://github.com/sercand/kuberesolver but more suitable for our needs.

Features:
* [x] K8s resolver that watches [endpoint API](https://kubernetes.io/docs/api-reference/v1.7/#endpoints-v1-core)
* [x] Different types of auth for kube-apiserver access. (You can run it easily from your local machine as well!)
* [x] URL in common kube-DNS format: `<service>.<namespace>(|.<any suffix>):<port|port name>`
 
Still todo:
* [ ] Metrics
* [ ] Fallback to SRV (?)
 
## Usage 

See [example](example/main.go) 

```go
resolver, err := k8sresolver.NewFromFlags()
if err != nil {
    // handle err.
}

watcher, err := resolver.Resolve(target)
if err != nil {
    // handle err.
}

// Wait for next updates.
updates, err := watcher.Next()
if err != nil {
    // handle err.
}
```
