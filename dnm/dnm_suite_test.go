package dnm_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestDnm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dnm Suite")
}
