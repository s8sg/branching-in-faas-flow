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
	dag := faasflow.CreateDag()

	// Set a dummy data
	dag.AddModifier("start-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking Start Node")
		if len(data) == 0 {
			data = []byte("aa-bb-cc")
		}
		log.Print("Starting data: ", string(data))
		return data, nil
	})

	// define the foreach branch dag as a node
	foreachdag := dag.AddForEachBranch("foreach-branch",
		// function that determine the status
		func(data []byte) map[string][]byte {
			log.Print("Invoking Foreach")

			splits := strings.Split(string(data), "-")
			option := make(map[string][]byte)

			for pos, item := range splits {
				// Each of these option will result in a different branch
				option[strconv.Itoa(pos)] = []byte(item)
			}
			return option
		},
		faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
			log.Print("Invoking Foreach Aggregator")

			result := ""
			for option, data := range results {
				result = result + " " + option + "=" + string(data)
			}
			return []byte(result), nil
		}),
	)
	foreachdag.AddModifier("node1", func(data []byte) ([]byte, error) {
		log.Print("Invoking foreach Node 1")
		result := fmt.Sprintf("foreach-node1(%s)", string(data))
		return []byte(result), nil
	})
	foreachdag.AddModifier("node2", func(data []byte) ([]byte, error) {
		log.Print("Invoking foreach Node 2")
		result := fmt.Sprintf("foreach-node2(%s)", string(data))
		return []byte(result), nil
	})
	foreachdag.AddEdge("node1", "node2")

	dag.AddModifier("parallel-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking Mod1 in parallel-node")
		result := fmt.Sprintf("parallel-node-mod1(%s)", string(data))
		return []byte(result), nil
	})
	dag.AddModifier("parallel-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking Mod2 in parallel-node")
		result := fmt.Sprintf("parallel-node-mod2(%s)", string(data))
		return []byte(result), nil
	})

	/* // XXX: Issue exists when two or more dynamic branch starts from or meets into same node
	conditiondags := dag.AddConditionalBranch("conditional-branch",
		// Conditions
		[]string{"condition1", "condition2"},
		// function that determine the status
		func(response []byte) []string {
			log.Print("Invoking Condition")

			// for each returned condition a seperate branch will execute
			return []string{"condition1", "condition2"}
		},
		// faasflow.ExecutionBranch, // delete this if intermidiate data need data store
		faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
			log.Print("Invoking Condition Aggregator")

			// results can be aggregated accross the branches
			result := ""
			for condition, data := range results {
				result = result + " " + condition + "=" + string(data)
			}
			return []byte(result), nil
		}),
	)
	conditiondags["condition1"].AddModifier("node1", func(data []byte) ([]byte, error) {
		log.Print("Invoking node1 under condition1")
		result := fmt.Sprintf("condition1-node1(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition1"].AddModifier("node2", func(data []byte) ([]byte, error) {
		log.Print("Invoking node2 in under condition1")
		result := fmt.Sprintf("condition1-node2(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition1"].AddEdge("node1", "node2")

	conditiondags["condition2"].AddModifier("node1", func(data []byte) ([]byte, error) {
		log.Print("Invoking node1 in under condition2")
		result := fmt.Sprintf("condition2-node1(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition2"].AddModifier("node2", func(data []byte) ([]byte, error) {
		log.Print("Invoking node2 in under condition2")
		result := fmt.Sprintf("condition2-node2(%s)", string(data))
		return []byte(result), nil
	})
	conditiondags["condition2"].AddEdge("node1", "node2")
	*/

	// AddVertex with Aggregator
	dag.AddVertex("end-node", faasflow.Aggregator(func(results map[string][]byte) ([]byte, error) {
		// results can be aggregated accross the branches
		log.Print("Invoking end-node Aggregator")
		result := ""
		for node, data := range results {
			result = result + " " + node + "=" + string(data)
		}
		return []byte(result), nil
	}))
	dag.AddModifier("end-node", func(data []byte) ([]byte, error) {
		log.Print("Invoking End Node")
		log.Print("End data: ", string(data))
		return data, nil
	})

	dag.AddEdge("start-node", "foreach-branch")
	dag.AddEdge("foreach-branch", "end-node")
	dag.AddEdge("start-node", "parallel-node")
	dag.AddEdge("parallel-node", "end-node")
	//dag.AddEdge("start-node", "conditional-branch")
	//dag.AddEdge("conditional-branch", "end-node")

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
	// initialize minio DataStore
	miniods, err := minioDataStore.InitFromEnv()
	if err != nil {
		return nil, err
	}
	return miniods, nil
}
