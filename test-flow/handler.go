package function

import (
	faasflow "github.com/s8sg/faas-flow"
	consulStateStore "github.com/s8sg/faas-flow-consul-statestore"
	"log"
	"os"
)

// Define provide definiton of the workflow
func Define(flow *faasflow.Workflow, context *faasflow.Context) (err error) {
	dag := faasflow.CreateDag()

	dag.AddModifier("start-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking Start Node")
		return data, nil
	})

	// define the foreach branch dag as a node
	foreachdag := dag.AddForEachBranch("dynamic-branch",
		// function that determine the status
		func(response []byte) map[string][]byte {
			return map[string][]byte{
				"1": response,
				"2": response,
				"3": response,
				// This executes dynamically
				// So no of entry in the map can be dynamic
			}
		},
		faasflow.ExecutionBranch, // delete this if intermidiate data need data store
		faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
			// results can be aggregated accross the branches
			return []byte(""), nil
		}),
	)
	foreachdag.AddModifier("foreach-node-1", func(data []byte) ([]byte, error) {
		log.Print("Invoking foreach Node 1")
		return data, nil
	})
	foreachdag.AddModifier("foreach-node-2", func(data []byte) ([]byte, error) {
		log.Print("Invoking foreach Node 2")
		return data, nil
	})
	foreachdag.AddEdge("foreach-node-1", "foreach-node-2", faasflow.Execution) // remove Execution tag if intermidiate data need datastore

	dag.AddModifier("end-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking End Node")
		return data, nil
	})
	dag.AddEdge("start-node", "dynamic-branch", faasflow.Execution) // remove Execution tag if intermidiate data need datastore
	dag.AddEdge("dynamic-branch", "end-node", faasflow.Execution)   // remove Execution tag if intermidiate data need datastore

	// set the dag in the flow
	flow.ExecuteDag(dag)

	return
}

// DefineStateStore provides the override of the default StateStore
func DefineStateStore() (faasflow.StateStore, error) {
	consulss, err := consulStateStore.GetConsulStateStore(
		os.Getenv("consul_url"),
		os.Getenv("consul_dc"),
	)
	if err != nil {
		return nil, err
	}
	return consulss, nil
}

// ProvideDataStore provides the override of the default DataStore
func DefineDataStore() (faasflow.DataStore, error) {
	return nil, nil
}
