package k8sresolver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"

	"github.com/Bplotka/go-tokenauth"
	"github.com/Bplotka/go-tokenauth/sources/direct"
	"github.com/Bplotka/go-tokenauth/sources/k8s"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

var (
	// FlagSet contains all the required flags for NewFromFlags to be working. Just use pflag.AddFlagSet in your setup if
	// you are willing to use it.
	// NOTE: Default values for all flags are designed for running within k8s pod.
	FlagSet = pflag.NewFlagSet("k8sresolver", pflag.ExitOnError)

	defaultKubeURL = fmt.Sprintf("https://%s", net.JoinHostPort(os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")))
	fKubeApiURL    = FlagSet.String("k8sresolver_kubeapi_url", defaultKubeURL,
		"TCP address to Kube API server in a form of 'http(s)://host:value'. If empty it will be fetched from env variables:"+
			"KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT")
	fInsecureSkipVerify = FlagSet.Bool("k8sresolver_tls_insecure", false, "If enabled, no server verification will be "+
		"performed on client side. Not recommended.")
	fKubeApiRootCAPath = FlagSet.String("k8sresolver_ca_file", defaultSACACert, "Path to service account CA file. "+
		"Required if kubeapi_tls_insecure = false.")

	// Different kinds of auth are supported. Currently supported with flags:
	// - specifying file with token
	// - specifying user (access) for kube config auth section to be reused (see
	// https://github.com/Bplotka/go-tokenauth/blob/88e9f6c7b19fa0ce19ab63476904e01417b53485/sources/k8s/k8s.go)
	fTokenAuthPath = FlagSet.String("k8sresolver_token_file", defaultSAToken,
		"Path to service account token to be used. This auth method has priority 2.")
	fKubeConfigAuthUser = FlagSet.String("k8sresolver_kubeconfig_user", "",
		"If user is specified resolver will try to fetch api auth method directly from kubeconfig. "+
			"This auth method has priority 1.")
	fKubeConfigAuthPath = FlagSet.String("k8sresolver_kubeconfig_path", "", "Kube config path. "+
		"Only used when k8sresolver_kubeconfig_user is specified. If empty it will try default path.")
)

// NewFromFlags creates resolver from flag from k8sresolver.FlagSet.
func NewFromFlags() (*resolver, error) {
	k8sURL := *fKubeApiURL
	if k8sURL == "" || k8sURL == "https://:" {
		return nil, errors.Errorf(
			"k8sresolver: k8sresolver_kubeapi_url flag needs to be specified or " +
				"KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined")
	}

	_, err := url.Parse(k8sURL)
	if err != nil {
		return nil, errors.Wrapf(err, "k8sresolver: k8sresolver_kubeapi_url flag needs to be valid URL. Value %s ", k8sURL)
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	if !*fInsecureSkipVerify {
		ca, err := ioutil.ReadFile(*fKubeApiRootCAPath)
		if err != nil {
			return nil, errors.Wrapf(err, "k8sresolver: failed to parse RootCA from file %s", *fKubeApiRootCAPath)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(ca)
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS10,
			RootCAs:    certPool,
		}
	}

	var source tokenauth.Source

	// Try kubeconfig auth first.
	if user := *fKubeConfigAuthUser; user != "" {
		source, err = k8sauth.New("kube_api", *fKubeConfigAuthPath, user)
		if err != nil {
			return nil, errors.Wrap(err, "k8sresolver: failed to create k8sauth Source")
		}
	}

	if source == nil {
		// Try token auth as fallback.
		token, err := ioutil.ReadFile(*fTokenAuthPath)
		if err != nil {
			return nil, errors.Wrapf(err, "k8sresolver: failed to parse token from %s. No auth method found", *fTokenAuthPath)
		}
		source = directauth.New("kube_api", string(token))
	}

	return New(k8sURL, source, tlsConfig), nil
}
