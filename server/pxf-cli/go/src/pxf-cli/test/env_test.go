package test

import (
	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"pxf-cli/env"
)

var _ = Describe("env.Require", func() {
	varname := "PXF_TEST_VAR"

	BeforeEach(func() {
		os.Unsetenv(varname)
	})

	AfterEach(func() {
		os.Unsetenv(varname)
	})

	It("Errors when the environment variable is not set", func() {
		value, err := env.Require(varname)
		Expect(value).To(Equal(""))
		Expect(err).To(Equal(errors.New("PXF_TEST_VAR is not set")))
	})

	It("Errors when the environment variable is blank", func() {
		os.Setenv(varname, "")
		value, err := env.Require(varname)
		Expect(value).To(Equal(""))
		Expect(err).To(Equal(errors.New("PXF_TEST_VAR is blank")))
	})

	It("Returns the value of the environment variable when it exists", func() {
		os.Setenv(varname, "foo")
		value, err := env.Require(varname)
		Expect(value).To(Equal("foo"))
		Expect(err).To(BeNil())
	})
})
