package function

import (
	"fmt"
	faasflow "github.com/s8sg/faas-flow"
	consulStateStore "github.com/s8sg/faas-flow-consul-statestore"
	minioDataStore "github.com/s8sg/faas-flow-minio-datastore"
	"log"
	"os"
	"strconv"
	"strings"
)

// Define provide definiton of the workflow
func Define(flow *faasflow.Workflow, context *faasflow.Context) (err error) {
	// Get flow dag
	dag := flow.Dag()

	// Set a dummy data
	dag.Node("start-node").Modify(func(data []byte) ([]byte, error) {
		log.Print("Invoking Start Node")
		if len(data) == 0 {
			data = []byte("aa-bb-cc")
		}
		log.Print("Starting data: ", string(data))
		return data, nil
	})

	// define the foreach branch dag as a node
	foreachdag := dag.ForEachBranch("foreach-branch",
		// function that determine the status
		func(data []byte) map[string][]byte {
			splits := strings.Split(string(data), "-")
			option := make(map[string][]byte)
			for pos, item := range splits {
				// Each of these option will result in a different branch
				option[strconv.Itoa(pos)] = []byte(item)
			}
			return option
		},
		faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
			result := ""
			for option, data := range results {
				result = result + " " + option + "=" + string(data)
			}
			return []byte(result), nil
		}),
	)
	foreachdag.Node("node1").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("foreach-node1(%s)", string(data))
		return []byte(result), nil
	})
	foreachdag.Node("node2").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("foreach-node2(%s)", string(data))
		return []byte(result), nil
	})
	foreachdag.Edge("node1", "node2")

	dag.Node("parallel-node").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("parallel-node-mod1(%s)", string(data))
		return []byte(result), nil
	}).Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("parallel-node-mod2(%s)", string(data))
		return []byte(result), nil
	})

	conditiondags := dag.ConditionalBranch("conditional-branch",
		// Conditions
		[]string{"condition1", "condition2"},
		// function that determine the status
		func(response []byte) []string {
			// for each returned condition a seperate branch will execute
			return []string{"condition1", "condition2"}
		},
		// faasflow.ExecutionBranch, // delete this if intermidiate data need data store
		faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
			// results can be aggregated accross the branches
			result := ""
			for condition, data := range results {
				result = result + " " + condition + "=" + string(data)
			}
			return []byte(result), nil
		}),
	)
	conditiondags["condition1"].Node("node1").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("condition1-node1(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition1"].Node("node2").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("condition1-node2(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition1"].Edge("node1", "node2")

	conditiondags["condition2"].Node("node1").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("condition2-node1(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition2"].Node("node2").Modify(func(data []byte) ([]byte, error) {
		result := fmt.Sprintf("condition2-node2(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition2"].Edge("node1", "node2")

	// AddVertex with Aggregator
	dag.Node("end-node", faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
		// results can be aggregated accross the branches
		result := ""
		for node, data := range results {
			result = result + " " + node + "=" + string(data)
		}
		return []byte(result), nil
	})).Modify(func(data []byte) ([]byte, error) {
		log.Print("Invoking End Node")
		log.Print("End data: ", string(data))
		return data, nil
	})

	dag.Edge("start-node", "foreach-branch")
	dag.Edge("foreach-branch", "end-node")
	dag.Edge("start-node", "parallel-node")
	dag.Edge("parallel-node", "end-node")
	dag.Edge("start-node", "conditional-branch")
	dag.Edge("conditional-branch", "end-node")

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
	// initialize minio DataStore
	miniods, err := minioDataStore.InitFromEnv()
	if err != nil {
		return nil, err
	}
	return miniods, nil
}
