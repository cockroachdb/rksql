package job

import "sync"

type workerPool struct {
	queue  chan func()
	stopCh chan struct{}
	wg     *sync.WaitGroup
}

func newWorkerPool(size int) *workerPool {
	wg := &sync.WaitGroup{}
	wg.Add(size)
	ret := workerPool{make(chan func(), 1000), make(chan struct{}), wg}
	for i := 0; i < size; i++ {
		go func() {
		Loop:
			for {
				select {
				case <-ret.stopCh:
					break Loop
				case f := <-ret.queue:
					f()
				}
			}
			wg.Done()
		}()
	}
	return &ret
}

func (wp *workerPool) submit(f func()) {
	wp.queue <- f
}

func (wp *workerPool) stopAndAwait() {
	close(wp.stopCh)
	wp.wg.Wait()
}
