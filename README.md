# flow-foreach-example
Super simple examples with dynamic branching in faas-flow


### Getting Started 
1. Deploy Openfaas
2. Deploy Consul as a statestore, follow : https://github.com/s8sg/faas-flow-consul-statestore
3. Review your configuration at `flow.yml`
```
environment:
  workflow_name: "test-branching"
  gateway: "gateway:8080"
  enable_tracing: false
  enable_hmac: false
  consul_url: "statestore_consul:8500"
  consul_dc: "dc1"
```
4. Deploy the flow-function
```
faas build
faas deploy
```
