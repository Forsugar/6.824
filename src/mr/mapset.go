package mr

import "log"

type MapSet struct {
	mapbool map[interface{}]bool
	count   int
}

func NewMapSet() *MapSet {
	m := MapSet{}
	m.mapbool = make(map[interface{}]bool)
	m.count = 0
	return &m
}

func (m *MapSet) Insert(data interface{}) {
	m.mapbool[data] = true
	m.count++
}

func (m *MapSet) Has(data interface{}) bool {
	return m.mapbool[data]
}

func (m *MapSet) Remove(data interface{}) {
	m.mapbool[data] = false
	m.count--
}

func (m *MapSet) Size() int {
	return m.count
}

func (m *MapSet) RemoveTimeOutTasksFromMapSet(tasks *BlockQueue, taskStates []TaskState, name string) {
	if m.count != 0 {
		for id, issued := range m.mapbool {
			currTime := getNowTimeSecond()
			if issued {
				if currTime-taskStates[id.(int)].BeginSecond > maxTaskTime {
					log.Printf(name+": worker %v on task %v abandoned due to timeout\n", taskStates[id.(int)].WorkId, id)
					m.Remove(id.(int))
					tasks.PutFront(id.(int))
				}
			}
		}
	}
}

func (m *MapSet) RemoveTimeoutMapTasks(mapTasks []MapTaskState, unIssuedMapTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-mapTasks[fileId.(int)].BeginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", mapTasks[fileId.(int)].WorkId, fileId)
				m.mapbool[fileId.(int)] = false
				m.count--
				unIssuedMapTasks.PutFront(fileId.(int))
			}
		}
	}
}

func (m *MapSet) removeTimeoutReduceTasks(reduceTasks []ReduceTaskState, unIssuedReduceTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTasks[fileId.(int)].BeginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", reduceTasks[fileId.(int)].WorkId, fileId)
				m.mapbool[fileId.(int)] = false
				m.count--
				unIssuedReduceTasks.PutFront(fileId.(int))
			}
		}
	}
}
