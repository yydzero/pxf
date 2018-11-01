package test

import (
	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"pxf-cli/pxf"
)

var _ = Describe("MakeValidClusterCommandInputs", func() {
	var oldGphome string
	var isGphomeSet bool

	BeforeEach(func() {
		oldGphome, isGphomeSet = os.LookupEnv("GPHOME")
		os.Setenv("GPHOME", "/test/gphome")
	})

	AfterEach(func() {
		if isGphomeSet {
			os.Setenv("GPHOME", oldGphome)
		} else {
			os.Unsetenv("GPHOME")
		}
	})

	It("Is successful when GPHOME is set and args are valid", func() {
		inputs, err := pxf.MakeValidClusterCommandInputs("init")
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome:  "/test/gphome",
			Command: "init",
		}))
	})

	It("Fails when GPHOME is not set", func() {
		os.Unsetenv("GPHOME")
		inputs, err := pxf.MakeValidClusterCommandInputs("init")
		Expect(err).To(Equal(errors.New("GPHOME is not set")))
		Expect(inputs).To(BeNil())
	})

	It("Fails when GPHOME is blank", func() {
		os.Setenv("GPHOME", "")
		inputs, err := pxf.MakeValidClusterCommandInputs("init")
		Expect(err).To(Equal(errors.New("GPHOME is blank")))
		Expect(inputs).To(BeNil())
	})

	It("Allows the start command", func() {
		inputs, err := pxf.MakeValidClusterCommandInputs("start")
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome:  "/test/gphome",
			Command: "start",
		}))
	})

	It("Allows the stop command", func() {
		inputs, err := pxf.MakeValidClusterCommandInputs("stop")
		Expect(err).To(BeNil())
		Expect(inputs).To(Equal(&pxf.CliInputs{
			Gphome:  "/test/gphome",
			Command: "stop",
		}))
	})

	It("Panics on an unknown command", func() {
		// We expect the 'flags' library to validate all CLI arguments for us,
		// so if an unknown command gets all the way to MakeValidClusterCommandInputs, it
		// must be programmer error
		Expect(func() { pxf.MakeValidClusterCommandInputs("yikes") }).To(Panic())
	})
})

var _ = Describe("RemoteCommandToRunOnSegments", func() {
	It("constructs a list of shell args from the input", func() {
		inputs := &pxf.CliInputs{
			Gphome:  "/test/gphome",
			Command: "init",
		}
		expected := []string{"/test/gphome/pxf/bin/pxf", "init"}

		Expect(pxf.RemoteCommandToRunOnSegments(inputs)).To(Equal(expected))
	})
})
