package kopiaup

import (
	"sync/atomic"

	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl"
)

type kopiaProgress struct {
	// all int64 must precede all int32 due to alignment requirements on ARM
	// +checkatomic
	uploadedBytes int64

	// +checkatomic
	uploadedFiles int32

	// +checkatomic
	ignoredErrorCount int32

	// +checkatomic
	fatalErrorCount int32

	estimatedFileCount int32 // +checklocksignore

	estimatedTotalBytes int64 // +checklocksignore

	upFunc func(upimpl.UploaderProgress)
}

func (p *kopiaProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.uploadedBytes, numBytes)
	atomic.AddInt32(&p.uploadedFiles, 1)

	p.ShowProgress()
}

func (p *kopiaProgress) Error(path string, err error, isIgnored bool) {
	if isIgnored {
		atomic.AddInt32(&p.ignoredErrorCount, 1)
	} else {
		atomic.AddInt32(&p.fatalErrorCount, 1)
	}
}

func (p *kopiaProgress) EstimatedDataSize(fileCount int, totalBytes int64) {
	atomic.StoreInt64(&p.estimatedTotalBytes, totalBytes)
	atomic.StoreInt32(&p.estimatedFileCount, int32(fileCount))

	p.ShowProgress()
}

func (p *kopiaProgress) ShowProgress() {
	p.upFunc(upimpl.UploaderProgress{
		TotalBytes: atomic.LoadInt64(&p.estimatedTotalBytes),
		BytesDone:  atomic.LoadInt64(&p.uploadedBytes),
	})
}

func (p *kopiaProgress) UploadStarted() {}

func (p *kopiaProgress) CachedFile(fname string, numBytes int64) {}

func (p *kopiaProgress) HashedBytes(numBytes int64) {}

func (p *kopiaProgress) HashingFile(fname string) {}

func (p *kopiaProgress) ExcludedFile(fname string, numBytes int64) {}

func (p *kopiaProgress) ExcludedDir(dirname string) {}

func (p *kopiaProgress) FinishedHashingFile(fname string, numBytes int64) {}

func (p *kopiaProgress) StartedDirectory(dirname string) {}

func (p *kopiaProgress) FinishedDirectory(dirname string) {}

func (p *kopiaProgress) UploadFinished() {}
