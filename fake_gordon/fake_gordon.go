package fake_gordon

import (
	"code.google.com/p/gogoprotobuf/proto"
	"github.com/nu7hatch/gouuid"
	"github.com/vito/gordon/warden"
)

type FakeGordon struct {
	Connected    bool
	ConnectError error

	CreatedHandles []string
	CreateError    error

	StopError error

	DestroyedHandles []string
	DestroyError     error

	SpawnError error

	LinkError error

	NetInError error

	LimitMemoryError error

	GetMemoryLimitError error

	LimitDiskError error

	GetDiskLimitError error

	ListError error

	InfoError error

	CopyInError error

	StreamError error

	RunError error
}

func New() *FakeGordon {
	f := &FakeGordon{}
	f.Reset()
	return f
}

func (f *FakeGordon) Reset() {
	f.Connected = false
	f.ConnectError = nil

	f.CreatedHandles = []string{}
	f.CreateError = nil

	f.StopError = nil

	f.DestroyedHandles = []string{}
	f.DestroyError = nil

	f.SpawnError = nil
	f.LinkError = nil
	f.NetInError = nil
	f.LimitMemoryError = nil
	f.GetMemoryLimitError = nil
	f.LimitDiskError = nil
	f.GetDiskLimitError = nil
	f.ListError = nil
	f.InfoError = nil
	f.CopyInError = nil
	f.StreamError = nil
	f.RunError = nil
}

func (f *FakeGordon) Connect() error {
	f.Connected = true
	return f.ConnectError
}

func (f *FakeGordon) Create() (*warden.CreateResponse, error) {
	if f.CreateError != nil {
		return nil, f.CreateError
	}

	handleUuid, _ := uuid.NewV4()
	handle := handleUuid.String()[:11]

	f.CreatedHandles = append(f.CreatedHandles, handle)

	return &warden.CreateResponse{
		Handle: proto.String(handle),
	}, nil
}

func (f *FakeGordon) Stop(handle string, background, kill bool) (*warden.StopResponse, error) {
	panic("NOOP!")
	return nil, f.StopError
}

func (f *FakeGordon) Destroy(handle string) (*warden.DestroyResponse, error) {
	if f.DestroyError != nil {
		return nil, f.DestroyError
	}

	f.DestroyedHandles = append(f.DestroyedHandles, handle)

	return &warden.DestroyResponse{}, nil
}

func (f *FakeGordon) Spawn(handle, script string, discardOutput bool) (*warden.SpawnResponse, error) {
	panic("NOOP!")
	return nil, f.SpawnError
}

func (f *FakeGordon) Link(handle string, jobID uint32) (*warden.LinkResponse, error) {
	panic("NOOP!")
	return nil, f.LinkError
}

func (f *FakeGordon) NetIn(handle string) (*warden.NetInResponse, error) {
	panic("NOOP!")
	return nil, f.NetInError
}

func (f *FakeGordon) LimitMemory(handle string, limit uint64) (*warden.LimitMemoryResponse, error) {
	panic("NOOP!")
	return nil, f.LimitMemoryError
}

func (f *FakeGordon) GetMemoryLimit(handle string) (uint64, error) {
	panic("NOOP!")
	return 0, f.GetMemoryLimitError
}

func (f *FakeGordon) LimitDisk(handle string, limit uint64) (*warden.LimitDiskResponse, error) {
	panic("NOOP!")
	return nil, f.LimitDiskError
}

func (f *FakeGordon) GetDiskLimit(handle string) (uint64, error) {
	panic("NOOP!")
	return 0, f.GetDiskLimitError
}

func (f *FakeGordon) List() (*warden.ListResponse, error) {
	panic("NOOP!")
	return nil, f.ListError
}

func (f *FakeGordon) Info(handle string) (*warden.InfoResponse, error) {
	panic("NOOP!")
	return nil, f.InfoError
}

func (f *FakeGordon) CopyIn(handle, src, dst string) (*warden.CopyInResponse, error) {
	panic("NOOP!")
	return nil, f.CopyInError
}

func (f *FakeGordon) Stream(handle string, jobID uint32) (<-chan *warden.StreamResponse, error) {
	panic("NOOP!")
	return nil, f.StreamError
}

func (f *FakeGordon) Run(handle, script string) (*warden.RunResponse, error) {
	panic("NOOP!")
	return nil, f.RunError
}
