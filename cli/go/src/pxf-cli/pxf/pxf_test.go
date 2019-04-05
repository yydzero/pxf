package pxf_test

import (
	"errors"
	"os"
	"pxf-cli/pxf"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CommandFunc", func() {
	BeforeEach(func() {
		_ = os.Setenv("GPHOME", "/test/gphome")
		_ = os.Setenv("PXF_CONF", "/test/gphome/pxf_conf")
	})

	It("Is successful when GPHOME and PXF_CONF are set and init is called", func() {
		commandFunc, err := pxf.Init.GetFunctionToExecute()
		Expect(err).To(BeNil())
		expected := "PXF_CONF=/test/gphome/pxf_conf /test/gphome/pxf/bin/pxf init"
		Expect(commandFunc("foo")).To(Equal(expected))
	})

	It("Is successful when GPHOME is set and start/stop are called", func() {
		commandFunc, err := pxf.Start.GetFunctionToExecute()
		Expect(err).To(BeNil())
		Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf start"))
		commandFunc, err = pxf.Stop.GetFunctionToExecute()
		Expect(err).To(BeNil())
		Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf stop"))
	})

	It("Fails to init or sync when PXF_CONF is not set", func() {
		_ = os.Unsetenv("PXF_CONF")
		commandFunc, err := pxf.Init.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
		commandFunc, err = pxf.Sync.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
	})

	It("Fails to init or sync when PXF_CONF is blank", func() {
		_ = os.Setenv("PXF_CONF", "")
		commandFunc, err := pxf.Init.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
		commandFunc, err = pxf.Sync.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
	})

	It("Fails to init, start, or stop when GPHOME is not set", func() {
		_ = os.Unsetenv("GPHOME")
		commandFunc, err := pxf.Init.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
		commandFunc, err = pxf.Start.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
		commandFunc, err = pxf.Stop.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME must be set")))
	})

	It("Fails to init, start, or stop when GPHOME is blank", func() {
		_ = os.Setenv("GPHOME", "")
		commandFunc, err := pxf.Init.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		commandFunc, err = pxf.Start.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		commandFunc, err = pxf.Stop.GetFunctionToExecute()
		Expect(commandFunc).To(BeNil())
		Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
	})

	It("sets up rsync commands of $PXF_CONF/{conf,lib,servers}", func() {
		commandFunc, err := pxf.Sync.GetFunctionToExecute()
		Expect(err).To(BeNil())
		Expect(commandFunc("sdw1")).To(Equal("rsync -az -e 'ssh -o StrictHostKeyChecking=no' '/test/gphome/pxf_conf/conf' '/test/gphome/pxf_conf/lib' '/test/gphome/pxf_conf/servers' 'sdw1:/test/gphome/pxf_conf'"))
		Expect(commandFunc("sdw2")).To(Equal("rsync -az -e 'ssh -o StrictHostKeyChecking=no' '/test/gphome/pxf_conf/conf' '/test/gphome/pxf_conf/lib' '/test/gphome/pxf_conf/servers' 'sdw2:/test/gphome/pxf_conf'"))
	})
})
