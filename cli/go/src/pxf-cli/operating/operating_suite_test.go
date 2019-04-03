package operating_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestOperating(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Operating Suite")
}
