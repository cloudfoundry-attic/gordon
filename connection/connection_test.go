package connection_test

import (
	"bytes"
	"math"
	"time"
	. "github.com/cloudfoundry-incubator/gordon/connection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.google.com/p/gogoprotobuf/proto"
	. "github.com/cloudfoundry-incubator/gordon/test_helpers"
	"github.com/cloudfoundry-incubator/gordon/warden"
)

var _ = Describe("Connection", func() {
	var (
		connection     *Connection
		writeBuffer    *bytes.Buffer
		wardenMessages []proto.Message
		resourceLimits *warden.ResourceLimits
	)

	assertWriteBufferContains := func(messages ...proto.Message) {
		ExpectWithOffset(1, string(writeBuffer.Bytes())).To(Equal(string(warden.Messages(messages...).Bytes())))
	}

	JustBeforeEach(func() {
		writeBuffer = bytes.NewBuffer([]byte{})

		fakeConn := &FakeConn{
			ReadBuffer:  warden.Messages(wardenMessages...),
			WriteBuffer: writeBuffer,
		}

		connection = New(fakeConn)
	})

	BeforeEach(func() {
		wardenMessages = []proto.Message{}
		resourceLimits = &warden.ResourceLimits{Nofile: proto.Uint64(72)}
	})

	Describe("Creating", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.CreateResponse{
					Handle: proto.String("foohandle"),
				},
			)
		})

		It("should create a container", func() {
			resp, err := connection.Create(map[string]string{
				"foo": "bar",
			})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(resp.GetHandle()).Should(Equal("foohandle"))

			assertWriteBufferContains(&warden.CreateRequest{
				Properties: []*warden.Property{
					{
						Key:   proto.String("foo"),
						Value: proto.String("bar"),
					},
				},
			})
		})
	})

	Describe("Stopping", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.StopResponse{},
			)
		})

		It("should stop the container", func() {
			_, err := connection.Stop("foo", true, true)
			Ω(err).ShouldNot(HaveOccurred())

			assertWriteBufferContains(&warden.StopRequest{
				Handle:     proto.String("foo"),
				Background: proto.Bool(true),
				Kill:       proto.Bool(true),
			})
		})
	})

	Describe("Destroying", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.DestroyResponse{},
			)
		})

		It("should stop the container", func() {
			_, err := connection.Destroy("foo")
			Ω(err).ShouldNot(HaveOccurred())

			assertWriteBufferContains(&warden.DestroyRequest{
				Handle: proto.String("foo"),
			})
		})
	})

	Describe("Limiting Memory", func() {
		Describe("Setting the memory limit", func() {
			BeforeEach(func() {
				wardenMessages = append(wardenMessages,
					&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)},
				)
			})

			It("should limit memory", func() {
				res, err := connection.LimitMemory("foo", 42)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(res.GetLimitInBytes()).Should(BeNumerically("==", 40))

				assertWriteBufferContains(&warden.LimitMemoryRequest{
					Handle:       proto.String("foo"),
					LimitInBytes: proto.Uint64(42),
				})
			})
		})

		Describe("Getting the memory limit", func() {
			Context("when the memory limit is well formatted", func() {
				BeforeEach(func() {
					wardenMessages = append(wardenMessages,
						&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)},
					)
				})

				It("should return the correct memory format", func() {
					memoryLimit, err := connection.GetMemoryLimit("foo")
					Ω(err).ShouldNot(HaveOccurred())
					Ω(memoryLimit).Should(BeNumerically("==", 40))

					assertWriteBufferContains(&warden.LimitMemoryRequest{
						Handle: proto.String("foo"),
					})
				})
			})

			Context("When the memory limit looks fishy", func() {
				BeforeEach(func() {
					wardenMessages = append(wardenMessages,
						&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(math.MaxInt64)},
					)
				})

				It("should return 0, without erroring", func() {
					memoryLimit, err := connection.GetMemoryLimit("foo")
					Ω(err).ShouldNot(HaveOccurred())
					Ω(memoryLimit).Should(BeNumerically("==", 0))

					assertWriteBufferContains(&warden.LimitMemoryRequest{
						Handle: proto.String("foo"),
					})
				})
			})
		})
	})

	Describe("Limiting Disk", func() {
		Describe("Setting the disk limit", func() {
			BeforeEach(func() {
				wardenMessages = append(wardenMessages,
					&warden.LimitDiskResponse{ByteLimit: proto.Uint64(40)},
				)
			})

			It("should limit disk", func() {
				res, err := connection.LimitDisk(&warden.LimitDiskRequest{
					Handle:    proto.String("foo"),
					ByteLimit: proto.Uint64(42),
				})

				Ω(err).ShouldNot(HaveOccurred())
				Ω(res.GetByteLimit()).Should(BeNumerically("==", 40))

				assertWriteBufferContains(&warden.LimitDiskRequest{
					Handle:    proto.String("foo"),
					ByteLimit: proto.Uint64(42),
				})
			})
		})

		Describe("Getting the disk limit", func() {
			BeforeEach(func() {
				wardenMessages = append(wardenMessages,
					&warden.LimitDiskResponse{ByteLimit: proto.Uint64(40)},
				)
			})

			It("should return the correct memory format", func() {
				diskLimit, err := connection.GetDiskLimit("foo")
				Ω(err).ShouldNot(HaveOccurred())
				Ω(diskLimit).Should(BeNumerically("==", 40))

				assertWriteBufferContains(&warden.LimitDiskRequest{
					Handle: proto.String("foo"),
				})
			})
		})
	})

	Describe("NetIn", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.NetInResponse{
					HostPort:      proto.Uint32(7331),
					ContainerPort: proto.Uint32(7332),
				},
			)
		})

		It("should return the allocated ports", func() {
			resp, err := connection.NetIn("foo-handle")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(resp.GetHostPort()).Should(BeNumerically("==", 7331))
			Ω(resp.GetContainerPort()).Should(BeNumerically("==", 7332))

			assertWriteBufferContains(&warden.NetInRequest{
				Handle: proto.String("foo-handle"),
			})
		})
	})

	Describe("Listing containers", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.ListResponse{
					Handles: []string{"container1", "container2", "container3"},
				},
			)
		})

		It("should return the list of containers", func() {
			resp, err := connection.List(map[string]string{"foo": "bar"})
			Ω(err).ShouldNot(HaveOccurred())

			Ω(resp.GetHandles()).Should(Equal([]string{"container1", "container2", "container3"}))

			assertWriteBufferContains(&warden.ListRequest{
				Properties: []*warden.Property{
					{
						Key:   proto.String("foo"),
						Value: proto.String("bar"),
					},
				},
			})
		})
	})

	Describe("Getting container info", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.InfoResponse{
					State: proto.String("active"),
				},
			)
		})

		It("should return the container's info", func() {
			resp, err := connection.Info("handle")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(resp.GetState()).Should(Equal("active"))

			assertWriteBufferContains(&warden.InfoRequest{
				Handle: proto.String("handle"),
			})
		})
	})

	Describe("Copying in", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.CopyInResponse{},
			)
		})

		It("should tell garden to copy", func() {
			_, err := connection.CopyIn("foo-handle", "/foo", "/bar")
			Ω(err).ShouldNot(HaveOccurred())

			assertWriteBufferContains(&warden.CopyInRequest{
				Handle:  proto.String("foo-handle"),
				SrcPath: proto.String("/foo"),
				DstPath: proto.String("/bar"),
			})
		})
	})

	Describe("Copying in", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.CopyOutResponse{},
			)
		})

		It("should tell garden to copy", func() {
			_, err := connection.CopyOut("foo-handle", "/foo", "/bar", "bartholofoo")
			Ω(err).ShouldNot(HaveOccurred())

			assertWriteBufferContains(&warden.CopyOutRequest{
				Handle:  proto.String("foo-handle"),
				SrcPath: proto.String("/foo"),
				DstPath: proto.String("/bar"),
				Owner:   proto.String("bartholofoo"),
			})
		})
	})

	Describe("When a connection error occurs", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.DestroyResponse{},
				//EOF
			)
		})

		It("should disconnect", func(done Done) {
			resp, err := connection.Destroy("foo-handle")
			Ω(err).ShouldNot(HaveOccurred())
			Ω(resp).ShouldNot(BeNil())

			<-connection.Disconnected
			close(done)
		})
	})

	Describe("Disconnecting", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.ErrorResponse{Message: proto.String("boo")},
			)
		})

		It("should error", func() {
			processID, resp, err := connection.Run("foo-handle", "echo hi", resourceLimits)
			Ω(processID).Should(BeZero())
			Ω(resp).Should(BeNil())
			Ω(err.Error()).Should(Equal("boo"))
		})
	})

	Describe("Round tripping", func() {
		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.EchoResponse{Message: proto.String("pong")},
			)
		})

		It("should do the round trip", func() {
			resp, err := connection.RoundTrip(
				&warden.EchoRequest{Message: proto.String("ping")},
				&warden.EchoResponse{},
			)

			Ω(err).ShouldNot(HaveOccurred())
			Ω(resp.(*warden.EchoResponse).GetMessage()).Should(Equal("pong"))
		})
	})

	Describe("Running", func() {
		stdout := warden.ProcessPayload_stdout
		stderr := warden.ProcessPayload_stderr

		Context("when running one process", func() {
			BeforeEach(func() {
				wardenMessages = append(wardenMessages,
					&warden.ProcessPayload{ProcessId: proto.Uint32(42)},
					&warden.ProcessPayload{ProcessId: proto.Uint32(42), Source: &stdout, Data: proto.String("1")},
					&warden.ProcessPayload{ProcessId: proto.Uint32(42), Source: &stderr, Data: proto.String("2")},
					&warden.ProcessPayload{ProcessId: proto.Uint32(42), ExitStatus: proto.Uint32(3)},
				)
			})

			It("should start the process and stream output", func(done Done) {
				processID, resp, err := connection.Run("foo-handle", "lol", resourceLimits)
				Ω(processID).Should(BeNumerically("==", 42))
				Ω(err).ShouldNot(HaveOccurred())

				assertWriteBufferContains(&warden.RunRequest{
					Handle:  proto.String("foo-handle"),
					Script:  proto.String("lol"),
					Rlimits: resourceLimits,
				})

				response1 := <-resp
				Ω(response1.GetSource()).Should(Equal(stdout))
				Ω(response1.GetData()).Should(Equal("1"))

				response2 := <-resp
				Ω(response2.GetSource()).Should(Equal(stderr))
				Ω(response2.GetData()).Should(Equal("2"))

				response3, ok := <-resp
				Ω(response3.GetExitStatus()).Should(BeNumerically("==", 3))
				Ω(ok).Should(BeTrue())

				Eventually(resp).Should(BeClosed())

				close(done)
			})
		})

		Context("spawning multiple processes", func() {
			BeforeEach(func() {
				wardenMessages = append(wardenMessages,
					&warden.ProcessPayload{ProcessId: proto.Uint32(42)},
					//we do this, so that the first Run has some output to read......
					&warden.ProcessPayload{ProcessId: proto.Uint32(42)},
					&warden.ProcessPayload{ProcessId: proto.Uint32(43)},
				)
			})

			It("should be able to spawn multiple processes sequentially", func() {
				processId, _, err := connection.Run("foo-handle", "echo hi", resourceLimits)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(processId).Should(BeNumerically("==", 42))

				assertWriteBufferContains(&warden.RunRequest{
					Handle:  proto.String("foo-handle"),
					Script:  proto.String("echo hi"),
					Rlimits: resourceLimits,
				})

				writeBuffer.Reset()

				time.Sleep(1 * time.Second)

				processId, _, err = connection.Run("foo-handle", "echo bye", resourceLimits)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(processId).Should(BeNumerically("==", 43))

				assertWriteBufferContains(&warden.RunRequest{
					Handle:  proto.String("foo-handle"),
					Script:  proto.String("echo bye"),
					Rlimits: resourceLimits,
				})
			})

		})
	})

	Describe("Attaching", func() {

		stdout := warden.ProcessPayload_stdout
		stderr := warden.ProcessPayload_stderr

		BeforeEach(func() {
			wardenMessages = append(wardenMessages,
				&warden.ProcessPayload{ProcessId: proto.Uint32(42), Source: &stdout, Data: proto.String("1")},
				&warden.ProcessPayload{ProcessId: proto.Uint32(42), Source: &stderr, Data: proto.String("2")},
				&warden.ProcessPayload{ProcessId: proto.Uint32(42), ExitStatus: proto.Uint32(3)},
			)
		})

		It("should stream", func(done Done) {
			resp, err := connection.Attach("foo-handle", 42)
			Ω(err).ShouldNot(HaveOccurred())

			assertWriteBufferContains(&warden.AttachRequest{
				Handle:    proto.String("foo-handle"),
				ProcessId: proto.Uint32(42),
			})

			response1 := <-resp
			Ω(response1.GetSource()).Should(Equal(warden.ProcessPayload_stdout))
			Ω(response1.GetData()).Should(Equal("1"))

			response2 := <-resp
			Ω(response2.GetSource()).Should(Equal(warden.ProcessPayload_stderr))
			Ω(response2.GetData()).Should(Equal("2"))

			response3 := <-resp
			Ω(response3.GetExitStatus()).Should(BeNumerically("==", 3))

			Eventually(resp).Should(BeClosed())

			close(done)
		})
	})
})
