package launcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
	cvelo_services "www.velocidex.com/golang/cloudvelo/services"
	"www.velocidex.com/golang/cloudvelo/services/client_info"
	"www.velocidex.com/golang/cloudvelo/testsuite"
	actions_proto "www.velocidex.com/golang/velociraptor/actions/proto"
	config_proto "www.velocidex.com/golang/velociraptor/config/proto"
	crypto_proto "www.velocidex.com/golang/velociraptor/crypto/proto"
	flows_proto "www.velocidex.com/golang/velociraptor/flows/proto"
	"www.velocidex.com/golang/velociraptor/json"
	"www.velocidex.com/golang/velociraptor/result_sets"
	"www.velocidex.com/golang/velociraptor/services"
	"www.velocidex.com/golang/velociraptor/utils"
	"www.velocidex.com/golang/velociraptor/vql/acl_managers"
	"www.velocidex.com/golang/velociraptor/vtesting"
)

const (
	getAllItemsQuery = `
{"query": {"match_all" : {}}}
`

	// Query to retrieve all the task queued for a client.
	getClientTasksQuery = `{
  "sort": [{
    "timestamp": {"order": "asc", "unmapped_type" : "long"}
  }],
  "query": {
    "bool": {
      "must": [
 		 {"match": {"doc_type" : "task"}},
         {"match": {"client_id" : %q}}
      ]}
  }
}
`
)

type LauncherTestSuite struct {
	*testsuite.CloudTestSuite
}

func (self *LauncherTestSuite) TestLauncher() {
	config_obj := self.ConfigObj.VeloConf()
	client_id := "C.1234"
	set_flow_id := "F.1234"

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	closer := utils.SetFlowIdForTests(set_flow_id)

	repository := self.loadTestArtifact(config_obj)

	self.seedClient(config_obj, client_id)

	acl_manager := acl_managers.NullACLManager{}
	flow_id, err := launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_manager,
		repository, &flows_proto.ArtifactCollectorArgs{
			ClientId:  client_id,
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.NoError(self.T(), err)

	assert.Equal(self.T(), flow_id, flow_id)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	// Wait here for the async flow to be written.
	vtesting.WaitUntil(5*time.Second, self.T(), func() bool {
		flows, _ := launcher.GetFlows(self.Ctx,
			config_obj, client_id, result_sets.ResultSetOptions{}, 0, 100)
		return len(flows.Items) > 0
	})

	// Get Flows API
	flows, err := launcher.GetFlows(self.Ctx,
		config_obj, client_id, result_sets.ResultSetOptions{}, 0, 100)
	assert.NoError(self.T(), err)

	assert.Equal(self.T(), 1, len(flows.Items))
	assert.Equal(self.T(), flow_id, flows.Items[0].SessionId)

	details, err := launcher.GetFlowDetails(self.Ctx, config_obj,
		services.GetFlowOptions{}, client_id, flow_id)
	assert.NoError(self.T(), err)

	// Make sure the flow is in the running state
	assert.Equal(self.T(),
		flows_proto.ArtifactCollectorContext_RUNNING,
		details.Context.State)

	// Check the requests are recorded
	requests, err := launcher.Storage().GetFlowRequests(
		self.Ctx, config_obj, client_id, flow_id, 0, 100)
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), 1, len(requests.Items))
	assert.NotNil(self.T(), requests.Items[0].FlowRequest)
	assert.True(self.T(), len(requests.Items[0].FlowRequest.VQLClientActions) > 0)

	// Make sure tasks are scheduled
	tasks, err := PeekClientTasks(self.Ctx, config_obj, client_id)
	assert.NoError(self.T(), err)

	assert.Equal(self.T(), 1, len(tasks))
	assert.Equal(self.T(), client_id, tasks[0].ClientId)
	assert.Equal(self.T(), flow_id, tasks[0].FlowId)

	// Now cancel the collection
	cancel_response, err := launcher.CancelFlow(
		self.Ctx, config_obj, client_id, flow_id, "admin")
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), flow_id, cancel_response.FlowId)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	// Check the tasks queue - we just add a cancel message but do not
	// remove the old message.
	tasks, err = PeekClientTasks(self.Ctx, config_obj, client_id)
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), 2, len(tasks))
	assert.Equal(self.T(), client_id, tasks[1].ClientId)

	message := &crypto_proto.VeloMessage{}
	err = protojson.Unmarshal([]byte(tasks[1].JSONData), message)
	assert.NoError(self.T(), err)
	assert.NotNil(self.T(), message.Cancel)

	// Make sure the collection is marked as cancelled now.
	details, err = launcher.GetFlowDetails(self.Ctx, config_obj,
		services.GetFlowOptions{}, client_id, flow_id)
	assert.NoError(self.T(), err)

	// Make sure the flow is in the error state
	assert.Equal(self.T(),
		flows_proto.ArtifactCollectorContext_ERROR,
		details.Context.State)

	// Create a second collection
	closer()

	closer = utils.SetFlowIdForTests(set_flow_id + "second")
	defer closer()

	flow_id, err = launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_manager,
		repository, &flows_proto.ArtifactCollectorArgs{
			ClientId:  client_id,
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	flows, err = launcher.GetFlows(self.Ctx,
		config_obj, client_id, result_sets.ResultSetOptions{}, 0, 100)
	assert.NoError(self.T(), err)

	assert.Equal(self.T(), uint64(2), flows.Total)
	assert.Equal(self.T(), 2, len(flows.Items))

	// Make sure the latest flow is first
	assert.Equal(self.T(), "F.1234second", flows.Items[0].SessionId)
}

// The server is a pseudo client with no record in the index - collections
// against it must still be scheduled.
func (self *LauncherTestSuite) TestScheduleServerCollection() {
	config_obj := self.ConfigObj.VeloConf()

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	closer := utils.SetFlowIdForTests("F.server")
	defer closer()

	repository := self.loadTestArtifact(config_obj)

	flow_id, err := launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_managers.NullACLManager{},
		repository, &flows_proto.ArtifactCollectorArgs{
			ClientId:  "server",
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), "F.server", flow_id)
}

// A client id that is well formed but has no record must still be refused,
// and the error must remain recognisable as a not found error.
func (self *LauncherTestSuite) TestScheduleUnknownClientRefused() {
	config_obj := self.ConfigObj.VeloConf()

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	closer := utils.SetFlowIdForTests("F.unknown")
	defer closer()

	repository := self.loadTestArtifact(config_obj)

	_, err = launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_managers.NullACLManager{},
		repository, &flows_proto.ArtifactCollectorArgs{
			ClientId:  "C.deadbeef",
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.Error(self.T(), err)
	assert.True(self.T(), utils.IsNotFound(err))
}

// A flow created after the client's flow list has been read must still be
// readable. The flow cache is keyed by client id and holds a point in time
// snapshot of that client's flows, so the new flow is absent from it.
func (self *LauncherTestSuite) TestGetFlowDetailsForFlowCreatedAfterCacheWarmed() {
	// The server pseudo client and a real agent are scheduled through
	// different branches, so both are covered.
	self.assertFlowReadableAfterCacheWarmed(
		"server", "F.serverwarm", "F.serverlate")
	self.assertFlowReadableAfterCacheWarmed(
		"C.cachewarm", "F.agentwarm", "F.agentlate")
}

// The datastore fallback must work even when nothing invalidates the cache.
// WriteFlow stores the record without rebuilding the index or purging the
// snapshot, so the read can only succeed by querying the datastore.
func (self *LauncherTestSuite) TestLoadCollectionContextFallsBackToDatastore() {
	config_obj := self.ConfigObj.VeloConf()
	client_id := "C.fallback"
	flow_id := "F.datastoreonly"

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	self.warmFlowCache(config_obj, client_id, "F.fallbackwarm")

	err = launcher.Storage().WriteFlow(self.Ctx, config_obj,
		&flows_proto.ArtifactCollectorContext{
			ClientId:      client_id,
			SessionId:     flow_id,
			State:         flows_proto.ArtifactCollectorContext_RUNNING,
			TotalRequests: 1,
		}, utils.SyncCompleter)
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	collection_context, err := launcher.Storage().LoadCollectionContext(
		self.Ctx, config_obj, client_id, flow_id)
	assert.NoError(self.T(), err)
	assert.NotNil(self.T(), collection_context)
	assert.Equal(self.T(), flow_id, collection_context.SessionId)
}

// A flow that exists nowhere must still be reported as a not found error,
// because that is what maps to a 404 rather than a 503.
func (self *LauncherTestSuite) TestLoadCollectionContextUnknownFlowIsNotFound() {
	config_obj := self.ConfigObj.VeloConf()

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	_, err = launcher.Storage().LoadCollectionContext(
		self.Ctx, config_obj, "C.noflows", "F.doesnotexist")
	assert.Error(self.T(), err)
	assert.True(self.T(), utils.IsNotFound(err))
}

func (self *LauncherTestSuite) assertFlowReadableAfterCacheWarmed(
	client_id, first_flow_id, second_flow_id string) {

	config_obj := self.ConfigObj.VeloConf()

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	// Populates the snapshot, which then holds the first flow but not the second.
	self.warmFlowCache(config_obj, client_id, first_flow_id)

	closer := utils.SetFlowIdForTests(second_flow_id)
	defer closer()

	flow_id, err := launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_managers.NullACLManager{},
		self.loadTestArtifact(config_obj),
		&flows_proto.ArtifactCollectorArgs{
			ClientId:  client_id,
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.NoError(self.T(), err)
	assert.Equal(self.T(), second_flow_id, flow_id)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	details, err := launcher.GetFlowDetails(self.Ctx, config_obj,
		services.GetFlowOptions{}, client_id, flow_id)
	assert.NoError(self.T(), err)
	assert.NotNil(self.T(), details.Context)
	assert.Equal(self.T(), flow_id, details.Context.SessionId)
}

// Schedule one flow for the client and read the flow list, which is what
// populates the client keyed cache entry.
func (self *LauncherTestSuite) warmFlowCache(
	config_obj *config_proto.Config, client_id, flow_id string) {

	launcher, err := services.GetLauncher(config_obj)
	assert.NoError(self.T(), err)

	if client_id != "server" {
		self.seedClient(config_obj, client_id)
	}

	closer := utils.SetFlowIdForTests(flow_id)
	defer closer()

	_, err = launcher.ScheduleArtifactCollection(
		self.Ctx, config_obj, acl_managers.NullACLManager{},
		self.loadTestArtifact(config_obj),
		&flows_proto.ArtifactCollectorArgs{
			ClientId:  client_id,
			Artifacts: []string{"TestArtifact"},
		}, nil)
	assert.NoError(self.T(), err)

	err = cvelo_services.FlushBulkIndexer()
	assert.NoError(self.T(), err)

	vtesting.WaitUntil(5*time.Second, self.T(), func() bool {
		flows, _ := launcher.GetFlows(self.Ctx, config_obj,
			client_id, result_sets.ResultSetOptions{}, 0, 100)
		return len(flows.Items) > 0
	})

	flows, err := launcher.GetFlows(self.Ctx, config_obj,
		client_id, result_sets.ResultSetOptions{}, 0, 100)
	assert.NoError(self.T(), err)
	assert.True(self.T(), len(flows.Items) > 0)
}

func (self *LauncherTestSuite) loadTestArtifact(
	config_obj *config_proto.Config) services.Repository {

	repository_manager, err := services.GetRepositoryManager(config_obj)
	assert.NoError(self.T(), err)

	repository := repository_manager.NewRepository()
	_, err = repository.LoadYaml(`
name: TestArtifact
sources:
- query: SELECT * FROM info()
`, services.ArtifactOptions{
		ValidateArtifact:  true,
		ArtifactIsBuiltIn: true})
	assert.NoError(self.T(), err)

	return repository
}

func (self *LauncherTestSuite) seedClient(
	config_obj *config_proto.Config, client_id string) {

	client_info_manager, err := services.GetClientInfoManager(config_obj)
	assert.NoError(self.T(), err)

	err = client_info_manager.Set(self.Ctx, &services.ClientInfo{
		ClientInfo: &actions_proto.ClientInfo{ClientId: client_id}})
	assert.NoError(self.T(), err)
}

func TestLauncher(t *testing.T) {
	suite.Run(t, &LauncherTestSuite{
		CloudTestSuite: &testsuite.CloudTestSuite{
			Indexes: []string{"persisted", "transient"},
		},
	})
}

func PeekClientTasks(ctx context.Context,
	config_obj *config_proto.Config,
	client_id string) (
	[]*client_info.ClientTask, error) {

	query := json.Format(getClientTasksQuery, client_id)
	hits, err := cvelo_services.QueryElastic(ctx, config_obj.OrgId,
		"persisted", query)
	if err != nil {
		return nil, err
	}

	results := []*client_info.ClientTask{}
	for _, hit := range hits {
		item := &client_info.ClientTask{}
		err = json.Unmarshal(hit.JSON, item)
		if err != nil {
			continue
		}
		results = append(results, item)
	}
	return results, nil
}
