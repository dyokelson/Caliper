// Copyright (c) 2021, Lawrence Livermore National Security, LLC.
// See top-level LICENSE file for details.

#include "caliper/CaliperService.h"

#include "../Services.h"

#include "caliper/Caliper.h"
#include "caliper/RegionProfile.h"
#include "caliper/SnapshotRecord.h"

#include "caliper/common/Log.h"
#include "caliper/common/OutputStream.h"
#include "caliper/common/RuntimeConfig.h"

#include "../../common/util/format_util.h"

#include <soma/Client.hpp>
#include <conduit/conduit.hpp>
#ifdef CALIPER_HAVE_MPI
    #include <mpi.h>
#endif

#include <chrono>
#include <iomanip>
#include <map>
#include <string>

#include <iostream>
#include <sstream>


#define SLURM_NOTIFY_TIMEOUT 5
static thallium::engine *engine;
static soma::Client *client;
static soma::CollectorHandle soma_collector;
static soma::NamespaceHandle *ns_handle;
static std::vector<thallium::async_response> requests;
static int my_rank = 0; 

using namespace cali;

namespace
{

std::string read_nth_line(const std::string& filename, int n)
{
   std::ifstream in(filename.c_str());

   std::string s;
   //for performance
   s.reserve(200);

   //skip N lines
   for(int i = 0; i < n; ++i)
       std::getline(in, s);

   std::getline(in,s);
   return s;
}

void initialize_soma_collector(int my_rank)
{
    const char* env_soma_address_fpath           = getenv("SOMA_SERVER_ADDR_FILE");
    const char* env_soma_node_fpath              = getenv("SOMA_NODE_ADDR_FILE");
    const char* env_soma_num_servers             = getenv("SOMA_NUM_SERVERS_PER_INSTANCE");
    const char* env_soma_server_id               = getenv("SOMA_SERVER_INSTANCE_ID");
    const char* env_soma_cali_mon_freq           = getenv("CALI_SOMA_MONITORING_FREQ");
    
    /* Parse and set up the connection information */
    int num_server = 1;
    num_server = std::stoi(std::string(env_soma_num_servers));
    int server_instance_id = std::stoi(std::string(env_soma_server_id));
    int my_server_offset = my_rank % num_server;
    std::string l = read_nth_line(env_soma_address_fpath, server_instance_id*num_server + my_server_offset + 1);
    size_t pos = 0;
    std::string delimiter = " ";
    pos = l.find(delimiter);
    std::string server_rank_str = l.substr(0, pos);
    std::stringstream s_(server_rank_str);
    int server_rank;
    s_ >> server_rank;
    l.erase(0, pos + delimiter.length());
    std::string address = l;
    unsigned provider_id = 0;
    std::string collector = read_nth_line(std::string(env_soma_node_fpath), server_instance_id*num_server + my_server_offset);
    std::string protocol = address.substr(0, address.find(":"));
    
    // Initialize the thallium server
    engine = new thallium::engine(protocol, THALLIUM_CLIENT_MODE);

    /* Create the soma connection handle */
    try {

        // Initialize a Client
        client = new soma::Client(*engine);
	    // Create the client handle 
        soma_collector = (*client).makeCollectorHandle(address, provider_id,
                    soma::UUID::from_string(collector.c_str()));
        ns_handle = soma_collector.soma_create_namespace("CALIPER");
        std::cout << "Initialized Namespace Handle" << std::endl;
        // Set publish frequency	
        int monitoring_frequency = 1;
        if (env_soma_cali_mon_freq != NULL) { 
            monitoring_frequency = std::atoi(env_soma_cali_mon_freq);
        }
        soma_collector.soma_set_publish_frequency(ns_handle, monitoring_frequency);
        std::cout << "CALIPER: Monitoring/publish frequency is: " << monitoring_frequency << std::endl;

    } catch(const soma::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

}

void write_soma_record(std::ostream& os, int mpi_rank, RegionProfile& profile, Caliper* c, SnapshotView rec)
{

    std::map<std::string, double> region_times;
    double total_time = 0;

    std::tie(region_times, std::ignore, total_time) =
        profile.exclusive_region_times();

    double unix_ts = 1e-6 * std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::string timestamp = std::to_string(unix_ts);
    std::string time_rank_key;
    time_rank_key = std::to_string(mpi_rank) + "/" + timestamp;

    for (const auto &p : region_times) {
        // ignore regions with < 5% of the epoch's total time
        // if (p.second < 0.05 * total_time)
        //     continue;

        soma_collector.soma_update_namespace(ns_handle, time_rank_key, p.first, p.second, soma::OVERWRITE); 
    }
    // Get other metrics as part of the record
    if (!rec.empty()) {
        for (const Entry& e : rec) {
            if (e.is_reference()) {
                std::cout << "Debugging this type of metric? " << e.node()->id() << std::endl;
            }
            else {
                std::string metric = c->get_attribute(e.attribute()).name_c_str();
                std::string value = e.value().to_string();
                soma_collector.soma_update_namespace(ns_handle, time_rank_key, metric, value, soma::OVERWRITE);
            }       
        }
    }
        
    auto response = soma_collector.soma_commit_namespace_async(ns_handle);
    if (response) {
        requests.push_back(*std::move(response));
    }

}

class SomaForwarder
{
    RegionProfile profile;
    OutputStream  stream;

    std::string   filename;

    void process_snapshot(Caliper* c, SnapshotView rec) {
        std::cout << "Intercepted Snapshot" << std::endl;
        
        write_soma_record(*stream.stream(), my_rank, profile, c, rec);

        profile.clear(); // reset profile - skip to create a cumulative profile
    }

    void post_init(Caliper* c, Channel* channel) {
        std::vector<Entry> rec;
        // initialize soma collector connection
        #ifdef CALIPER_HAVE_MPI
            MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
        #endif
        initialize_soma_collector(my_rank);

        stream.set_filename(filename.c_str(), *c, rec);
        profile.start();
    }
    
    void finalize() {
        // shutdown hook - finish_evt to flush data to SOMA
        for(auto i = requests.begin(); i != requests.end(); i++) {
            i->wait();
        }
        // write to file at the very end
        if (my_rank == 0) {
            std::cout << "Writing to File" << std::endl;
            std::string outfile = "caliper_data_soma.txt";
            bool write_done;
            soma_collector.soma_write(outfile, &write_done, soma::OVERWRITE);
        }
        
    }

    SomaForwarder(const char* fname)
        : filename { fname }
    { }

public:
    
    static const char* s_spec;

    static void create(Caliper* c, Channel* channel) {
        ConfigSet cfg = 
            services::init_config_from_spec(channel->config(), s_spec);

        SomaForwarder* instance = new SomaForwarder(cfg.get("filename").to_string().c_str());

        channel->events().post_init_evt.connect(
            [instance](Caliper* c, Channel* channel){
                instance->post_init(c, channel);
            });
        /* channel->events().snapshot.connect(
             [instance](Caliper* c, Channel* channel, SnapshotView, SnapshotBuilder&){
                 instance->snapshot(c, channel);
             }); */
        channel->events().process_snapshot.connect(
             [instance](Caliper* c, Channel* channel, SnapshotView, SnapshotView record){
                 instance->process_snapshot(c, record);
             });
        channel->events().finish_evt.connect(
            [instance](Caliper* c, Channel* chn){
                instance->finalize();
                delete instance;
            });

        Log(1).stream() << channel->name() << "Initialized SOMA forwarder\n";
    }

	
};

const char* SomaForwarder::s_spec = R"json(
{   
 "name"        : "soma",
 "description" : "Forward Caliper regions to SOMA (prototype)",
 "config"      :
 [
  {
    "name"        : "filename",
    "description" : "Output file name, or stdout/stderr",
    "type"        : "string",
    "value"       : "stdout"
  }
 ]
}
)json";

} // namespace [anonymous]

namespace cali
{

CaliperService soma_service { ::SomaForwarder::s_spec, ::SomaForwarder::create };

}
