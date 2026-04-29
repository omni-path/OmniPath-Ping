/* -*- P4_16 -*- */
#include <core.p4>
#include <tna.p4>
// SWITCH_ID reserves the lower 12 bits for the port field.
#define SWITCH_ID 4096
#define PORT_141 300
#define CIRC_PORT_0 68
#define CIRC_PORT_1 196
#define DELTA_MAX 10000000 // Unit: 128 ns
#define PORT_1 196
#define PORT_2 197
#define PORT_3 198
#define PORT_4 199

#define BSP_SIZE 4096
// #define PORT_1 68
// #define PORT_2 69
// #define PORT_3 70
// #define PORT_4 71

extern void assign(inout bit<32> dst, bit<32> src);
/*************************************************************************
 ************* C O N S T A N T S    A N D   T Y P E S  *******************
*************************************************************************/
enum bit<16> ether_type_t {
    TPID       = 0x8100,
    IPV4       = 0x0800,
    ROCE       = 0x8915,
    MINE       = 0x8888,
    CIRC       = 0x8889,
    ARP        = 0x0806,
    HBIT       = 0x888a,
    PONG       = 0x888b,
    FIRST_PONG       = 0x888c
}

enum bit<8>  ip_proto_t {
    ICMP  = 1,
    IGMP  = 2,
    TCP   = 6,
    UDP   = 17
}
struct ports {
    bit<16>  sp;
    bit<16>  dp;
}
struct bsp_timeout_pair_h {
    bit<32>     timeout_flag;
    bit<32>     timeout_time;
}

type bit<48> mac_addr_t;

/*************************************************************************
 ***********************  H E A D E R S  *********************************
 *************************************************************************/
/*  Define all the headers the program will recognize             */
/*  The actual sets of headers processed by each gress can differ */

/* Standard ethernet header */
header ethernet_h {
    mac_addr_t    dst_addr;
    mac_addr_t    src_addr;
    ether_type_t  ether_type;
}
header next_hop_bit_h {
    bit<32>    bitval;
}
header circ_h {
    bit<16> task_ID;
}


header ping_mirror_h
{
    mac_addr_t    dst_addr;
    mac_addr_t    src_addr;
    ether_type_t  ether_type;
    bit<32>       bitval;
}
header circ_mirror_h
{
    mac_addr_t    dst_addr;
    mac_addr_t    src_addr;
    ether_type_t  ether_type;
}
header pong_mirror_h
{
    mac_addr_t    dst_addr;
    mac_addr_t    src_addr;
    ether_type_t  ether_type;
    bit<32>    wait_ports;
    //now28
}
header arp_h {
    bit<16>       htype;
    bit<16>       ptype;
    bit<8>        hlen;
    bit<8>        plen;
    bit<16>       opcode;
    mac_addr_t    hw_src_addr;
    bit<32>       proto_src_addr;
    mac_addr_t    hw_dst_addr;
    bit<32>       proto_dst_addr;
}

header ipv4_h {
    bit<4>       version;
    bit<4>       ihl;
    bit<7>       diffserv;
    bit<1>       res;
    bit<16>      total_len;
    bit<16>      identification;
    bit<3>       flags;
    bit<13>      frag_offset;
    bit<8>       ttl;
    bit<8>   protocol;
    bit<16>      hdr_checksum;
    bit<32>  src_addr;
    bit<32>  dst_addr;// dstip
}

header icmp_h {
    bit<16>  type_code;
    bit<16>  checksum;
}

header igmp_h {
    bit<16>  type_code;
    bit<16>  checksum;
}

header tcp_h {
    bit<16>  src_port;
    bit<16>  dst_port;
    bit<32>  seq_no;
    bit<32>  ack_no;
    bit<4>   data_offset;
    bit<4>   res;
    bit<8>   flags;
    bit<16>  window;
    bit<16>  checksum;
    bit<16>  urgent_ptr;
}

header udp_h {
    bit<16>  src_port;
    bit<16>  dst_port;
    bit<16>  len;
    bit<16>  checksum;
}
header circ_t{
    bit<16>     task_ID;
    bit<32>     wait_ports;
}
// PING payload fields (23 bytes):
// 1. Task ID, 2 bytes (provided by the endpoint).
// 2. Task start timestamp, 6 bytes (provided by the source ToR).
// 3. In-switch loopback count, 2 bytes (reset at each switch).
// 4. Next-hop egress-port bitmap, 4 bytes (looked up by the switch).
// 5. Hop count, 1 byte (configured by the switch; starts as total hops and
//    decrements at each switch).
// 6. Previous-hop delay, 4 bytes (captured at dequeue).
// 7. Previous-hop link information, 4 bytes (captured at dequeue).
header ping_payload_h{
    bit<16>     task_ID;
    bit<32>     task_start_tstamp;
    bit<16>     circ_times;
    bit<32>     next_hop_bit;
    bit<8>      hop_times;
    bit<32>     last_hop_delay;
    bit<32>     last_hop_link;
}

// PONG payload fields (23 bytes):
// 1. Task ID, 2 bytes (reused from the PING packet).
// 2. Task start timestamp, 6 bytes (reused from the PING packet).
// 4. Path count, 2 bytes (aggregated).
// 5. Maximum end-to-end path delay, 4 bytes (aggregated).
// 6. Maximum-delay link information, 4 bytes (aggregated).
// 7. Drop-link information, 4 bytes (aggregated).
header pong_payload_h{
    bit<16>     task_ID;
    bit<32>     task_start_tstamp;
    bit<16>     path_count;
    bit<32>     max_delay;
    bit<32>     max_delay_link;
    bit<32>     drop_link;
}
// Loopback feasibility check: rotate over task_ID values.
//
header my_ping_payload_h {
}
header my_pong_wait_ports_h {
    bit<32>     wait_ports;
}




/*************************************************************************
 **************  I N G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/
 
    /***********************  H E A D E R S  ************************/

struct my_ingress_headers_t{
    ethernet_h         ethernet_2;
    my_pong_wait_ports_h    my_pong_wait_ports;
    ethernet_h         ethernet;
    arp_h              arp;
    ipv4_h             ipv4;
    icmp_h             icmp;
    igmp_h             igmp;
    tcp_h              tcp;
    udp_h              udp;
    ping_payload_h     ping_payload;
    pong_payload_h     pong_payload;
    
    
}


    /******  G L O B A L   I N G R E S S   M E T A D A T A  *********/
// Flow-table bucket state.
struct flow_bucket_t {
    bit<32> current_timestamp; // Current timestamp
    bit<32> max_time_interval; // Maximum time interval
    bit<16> fingerprint;       // Flow fingerprint
    bit<16> useless;           // Padding
}

struct my_ingress_metadata_t {
    bit<32>     port_bit;
    bit<2> ecmp;
    bit<32>     ll;
    bit<1>      bsp_is_circ;
    bit<1>      have_circled;
    ping_mirror_h  ping_mirror; // Extra ping mirror header
    circ_mirror_h circ_mirror;
    MirrorId_t session_id;
    bit<32>  next_port_bit;
    PortId_t  next_port;

    bit<32>  src_addr; // Source IP
    bit<32>  dst_addr; // Destination IP

    bit<16>  src_port;
    bit<16>  dst_port; // Destination port

    // BSP table-related fields.
    bit<16>     bsp_task_id;          // BSP task ID
    bit<32>     bsp_timestamp;        // BSP task timestamp
    bit<32>     bsp_timestamp_check;  // BSP task timestamp comparison result
    bit<32>     bsp_waiting_ports;    // Waiting-port bitmap
    bit<32>     bsp_forwarding_ports; // Forwarding-port bitmap
    bit<16>     bsp_path_count;       // Path-count accumulator
    bit<32>     bsp_max_delay;        // Maximum end-to-end path delay
    bit<32>     bsp_max_delay_link;   // Maximum-delay link information
    bit<32>     bsp_drop_link;        // Drop-link information
    bsp_timeout_pair_h  bsp_timeout;
    
    bit<16>     bsp_circ_times;     // Loopback count for ping loopback broadcast
    bit<32>     bsp_src_ip;         // Source IP
    bit<32>     bsp_dst_ip;         // Destination IP
    bit<16>     bsp_src_port;       // Source port
    bit<16>     bsp_dst_port;       // Destination port
    bit<32>     bsp_sp_dp;          // src_port + dst_port
    bit<1>      bsp_is_pong;        // Whether the packet is a PONG packet
    bit<32>     bsp_timeout_interval; // Timeout interval configured by table
    bit<32>      bsp_max_delay_check;  // Staged max-delay update
    

}

    /***********************  P A R S E R  **************************/

parser IngressParser(packet_in        pkt,
    /* User */
    out my_ingress_headers_t          hdr,
    out my_ingress_metadata_t         meta,
    /* Intrinsic */
    out ingress_intrinsic_metadata_t  ig_intr_md)
{
    state start {
        pkt.extract(ig_intr_md);
        pkt.advance(PORT_METADATA_SIZE);
        transition parse_ethernet;
    }
    state parse_ethernet {
        meta.ecmp = 0;
        pkt.extract(hdr.ethernet);
  
        transition select((bit<16>)hdr.ethernet.ether_type) {
            (bit<16>)ether_type_t.IPV4            :  parse_ipv4;
            (bit<16>)ether_type_t.ROCE            :  parse_ipv4;
            (bit<16>)ether_type_t.ARP             :  parse_arp;
            default :  accept;
        }
    }
    state parse_arp {
        pkt.extract(hdr.arp);
        transition accept;
    }

    state parse_ipv4 {
        meta.bsp_timestamp_check = 0; // Initial value means unset.
        meta.bsp_path_count = 0;
        pkt.extract(hdr.ipv4);
        meta.src_addr = hdr.ipv4.src_addr;
        meta.dst_addr = hdr.ipv4.dst_addr;
        transition select(hdr.ipv4.protocol) {
            1 : parse_icmp;
            2 : parse_igmp;
            6 : parse_tcp;
           17 : parse_udp;
           
           16 : parse_udp;// 
          100 : parse_udp;//
          101 : parse_udp;//
            default : accept;
        }
    }


    state parse_icmp {
        meta.ll=pkt.lookahead<bit<32>>();
        pkt.extract(hdr.icmp);
        transition accept;
    }
    
    state parse_igmp {
        meta.ll=pkt.lookahead<bit<32>>();
        pkt.extract(hdr.igmp);
        transition accept;
    }
    
    state parse_tcp {
        meta.ll=pkt.lookahead<bit<32>>();
        pkt.extract(hdr.tcp);
        transition accept;
    }
    
    state parse_udp {
        meta.ll=pkt.lookahead<bit<32>>();
        pkt.extract(hdr.udp);
        meta.src_port = hdr.udp.src_port;
        meta.dst_port = hdr.udp.dst_port;
        transition select(hdr.ipv4.protocol) {           
           16 : parse_ping_payload;// 
          100 : parse_pong_payload;//
          101 : parse_circ_payload;//
            default : accept;
        }
    }
    
    state parse_ping_payload{
        pkt.extract(hdr.ping_payload);
        
        meta.bsp_task_id = hdr.ping_payload.task_ID;
        meta.bsp_timestamp = hdr.ping_payload.task_start_tstamp;
        // meta.bsp_hop_times =  hdr.ping_payload.hop_times;
        meta.bsp_max_delay =  hdr.ping_payload.last_hop_delay;
        meta.bsp_max_delay_link =  hdr.ping_payload.last_hop_link;
        meta.bsp_circ_times = hdr.ping_payload.circ_times;
        transition accept;
    }
    state parse_pong_payload{
        pkt.extract(hdr.pong_payload);
        // Extract the task ID and other PONG fields.
        
        meta.bsp_circ_times = 1;
        meta.bsp_is_pong = 1;

        meta.bsp_task_id = hdr.pong_payload.task_ID;
        meta.bsp_timestamp = (bit<32>)hdr.pong_payload.task_start_tstamp;
        
        meta.bsp_path_count =  hdr.pong_payload.path_count;
        meta.bsp_max_delay =  hdr.pong_payload.max_delay;
        meta.bsp_max_delay_link =  hdr.pong_payload.max_delay_link;
        meta.bsp_drop_link =  hdr.pong_payload.drop_link;
        transition accept;
    }
    state parse_circ_payload
    {
        pkt.extract(hdr.pong_payload);
        meta.bsp_is_circ=1;
        meta.bsp_circ_times=1;
        meta.bsp_timestamp_check = 1;
        meta.bsp_task_id = hdr.pong_payload.task_ID;
    }
    
}



const bit<8> MAX_FLOWS = 128;











control Ingress(/* User */
    inout my_ingress_headers_t                       hdr,
    inout my_ingress_metadata_t                      meta,
    /* Intrinsic */ 
    in    ingress_intrinsic_metadata_t               ig_intr_md,
    in    ingress_intrinsic_metadata_from_parser_t   ig_prsr_md,
    inout ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md,
    inout ingress_intrinsic_metadata_for_tm_t        ig_tm_md)
{
    CRCPolynomial<bit<32>>(0x04C11DB7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32a;
    CRCPolynomial<bit<32>>(0x741B8CD7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32b;
    CRCPolynomial<bit<32>>(0xDB710641,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32c;
    CRCPolynomial<bit<32>>(0x82608EDB,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32fp;


    Hash<bit<5>>(HashAlgorithm_t.CUSTOM,crc32c) hash_rand;
    Hash<bit<2>>(HashAlgorithm_t.CUSTOM,crc32c) hash_ecmp;
    Hash<bit<32>>(HashAlgorithm_t.IDENTITY,crc32c) hash_identity;


    // Empty action.
    action no_action() {
        // Intentionally empty.
    }

    /* normal ipv4 packets processing */
    action cal_ecmp() // Compute ECMP.
    {
        meta.ecmp=hash_ecmp.get({ig_intr_md.ingress_mac_tstamp[3:0]});
        
        // Record src_port and dst_port.
        meta.bsp_sp_dp[31:16] = hdr.udp.src_port;
    }
    @stage(0)  table cal_ecmp_t
    {
        actions={cal_ecmp;}
        default_action=cal_ecmp;
    }
    action ecmp_select(PortId_t port,bit<32> port_bit)
    {
        meta.next_port=port;
        meta.next_port_bit= hdr.ping_payload.next_hop_bit & port_bit;


        //others
        meta.bsp_sp_dp[15:0]  = hdr.udp.dst_port; 
        meta.bsp_is_pong = meta.bsp_is_pong&meta.bsp_timestamp_check[0:0];
    }
    @stage(1)  table ipv4_host_t
    {   
        key={hdr.ipv4.dst_addr:exact;meta.ecmp:exact;}
        actions={ecmp_select;no_action;}
        default_action=no_action;
        size=100;
    }

    /* arp packets processing */
    action unicast_send(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
        ig_tm_md.bypass_egress=1;
    }
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }
    @stage(0) table arp_host {
        
        key = { hdr.arp.proto_dst_addr : exact; }
        actions = { unicast_send; drop; }
        default_action = drop();
    }
    /* ping packets processing */

    action port_to_bit_forward_a(bit<32> b)
    {
        meta.bsp_forwarding_ports = b;
        
        meta.bsp_timeout.timeout_time = (bit<32>)ig_intr_md.ingress_mac_tstamp[46:16];
    }
   
    @stage(0)  table ingress_port_to_bit_forward_t // Ingress port -> port bit, added to forwarding_ports.
    {
        key={
            ig_intr_md.ingress_port:exact;
        }
        actions={
            port_to_bit_forward_a;
        }
        default_action=port_to_bit_forward_a(0);
        size=100;
    } action port_to_bit_waiting_a(bit<32> b)
    {
        meta.bsp_waiting_ports = b;
    }
    @stage(0)  table ingress_port_to_bit_waiting_t // Ingress port -> port bit, added to wait_ports.
    {
        key={
            ig_intr_md.ingress_port:exact;
        }
        actions={
            port_to_bit_waiting_a;
        }
        default_action=port_to_bit_waiting_a(0);
        size=100;
    }



    action get_high_bit_port_a(bit<32> port)
    {
        meta.bsp_drop_link[11:0] =  port[11:0]; // drop_link
    }
    @stage(5)  table get_high_bit_port_t // Port bit -> highest-bit port.
    {
        key={meta.bsp_forwarding_ports:ternary;}
        actions={get_high_bit_port_a;}
        default_action=get_high_bit_port_a(0);
        size=100;
    }
    action bitmap_select(bit<32> bitmap)
    {
        hdr.ping_payload.next_hop_bit=bitmap;
        // meta.bsp_forwarding_ports = bitmap;
    }
    @stage(0)  table ipv4_host_ping_t
    {
        key={hdr.ipv4.dst_addr:exact;}
        actions={bitmap_select;}
        default_action=bitmap_select(0);
        size=100;
    }

    action ping_forwarding_ports_check_a()
    {
        meta.bsp_forwarding_ports = hdr.ping_payload.next_hop_bit;
    }
    @stage(1)  table ping_forwarding_ports_check_t
    {
        actions={ping_forwarding_ports_check_a;}
        default_action=ping_forwarding_ports_check_a;
    }

    action pkt_send_to_port_a()
    {
        hdr.ping_payload.next_hop_bit = hdr.ping_payload.next_hop_bit^meta.next_port_bit;
        ig_tm_md.ucast_egress_port=meta.next_port;
        hdr.ipv4.ttl=hdr.ipv4.ttl-1;
    }
    @stage(2)  table pkt_send_to_port_t{
        actions={pkt_send_to_port_a;}
        default_action=pkt_send_to_port_a;
    }
    action pkt_send_to_circ_a()
    {
        ig_tm_md.ucast_egress_port=CIRC_PORT_1;
    }
    @stage(2)  table pkt_send_to_circ_t{
        actions={pkt_send_to_circ_a;}
        default_action=pkt_send_to_circ_a;
    }
    action cal_timeout_time_a(bit<32> time_delay)
    {
        meta.bsp_timeout.timeout_time = time_delay + (bit<32>)ig_intr_md.ingress_mac_tstamp[46:16];
    }
    @stage(0)  table cal_timeout_time_t// x -> 1<<x
    {
        key={hdr.ping_payload.hop_times:exact;}
        actions={cal_timeout_time_a;}
        default_action=cal_timeout_time_a(0);
        size=32;
    }

    action circle_ping_a() { // Dedicated port for ping loopback broadcast.
        meta.ping_mirror.ether_type = ether_type_t.HBIT;
        meta.ping_mirror.bitval = hdr.ping_payload.next_hop_bit;
        ig_dprsr_md.mirror_type = 1;
        meta.session_id = 1;
    }
    @stage(3)  table circle_ping_t // Simulate loopback broadcast.
    {
        actions={circle_ping_a;}
        default_action=circle_ping_a;
    }

    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_timestamp; // Stage 1, uses two register actions.
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_waiting_ports; // Stage 2, uses one register action.
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_forwarding_ports; // Stage 3, uses one register action.
    
    Register<bit<16>, bit<16>>(BSP_SIZE, 0) bsp_path_count;
    
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_max_delay;
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_max_delay_link;
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_drop_link;

    Register<bsp_timeout_pair_h, bit<16>>(BSP_SIZE)bsp_timeout_flag_time;
    
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_src_ip;
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_dst_ip;
// struct ports {    bit<16>  sp;    bit<16>  dp;}
    Register<bit<32>, bit<16>>(BSP_SIZE) bsp_src_dst_port;


    Register<bit<32>, bit<16>>(BSP_SIZE, 0) circs_ports; // Stage 10, uses one register action.
    RegisterAction<bit<32>, bit<16>,bit<32>>(circs_ports) set_port = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            register_data = (bit<32>) ig_tm_md.ucast_egress_port;
            result = register_data; // get
        }
    };
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) circs_final_bits; // Stage 10, uses one register action.
    RegisterAction<bit<32>, bit<16>,bit<32>>(circs_final_bits) set_final_bits = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            register_data = (bit<32>) ig_dprsr_md.drop_ctl;
            result = register_data; // get
        }
    };

    Register<bit<32>, bit<16>>(BSP_SIZE, 0) circs_first_bits; // Stage 10, uses one register action.
    RegisterAction<bit<32>, bit<16>,bit<32>>(circs_first_bits) set_first_bits = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            register_data = (bit<32>) hdr.ping_payload.next_hop_bit;
            result = register_data; // get
        }
    };
// 0. BSP IP and port operations.
    // src ip;
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_src_ip) set_bsp_src_ip = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // set
                register_data = hdr.ipv4.src_addr;
            }
            result = register_data; // get
        }
    };
    action set_bsp_src_ip_a() {
        meta.bsp_src_ip = set_bsp_src_ip.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(3) table set_bsp_src_ip_t {
        actions = { set_bsp_src_ip_a; }
        default_action = set_bsp_src_ip_a;
    }

    // dst ip;
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_dst_ip) set_bsp_dst_ip = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // set
                register_data = hdr.ipv4.dst_addr;
            }
            result = register_data;
        }
    };
    action set_bsp_dst_ip_a() {
        meta.bsp_dst_ip = set_bsp_dst_ip.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(3) table set_bsp_dst_ip_t {
        actions = { set_bsp_dst_ip_a; }
        default_action = set_bsp_dst_ip_a;
    }
    // src port;
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_src_dst_port) set_bsp_src_dst_port = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // set
                register_data =  meta.bsp_sp_dp;
            }
            result = register_data;
        }
    };
    action set_bsp_src_dst_port_a() {
        meta.bsp_sp_dp = set_bsp_src_dst_port.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(3) table set_bsp_src_dst_port_t {
        actions = { set_bsp_src_dst_port_a; }
        default_action = set_bsp_src_dst_port_a;
    }

// 1. BSP timestamp operations.


    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_timestamp) check_set_bsp_timestamp = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(register_data < meta.bsp_timestamp){
                register_data =  meta.bsp_timestamp;
                result = 2;
            }
            else if(register_data ==  meta.bsp_timestamp){
                result = 1;// no timestamp change
            }
            // else{
            //     result = 3;
            // }
        }
    };
    action check_set_bsp_timestamp_a() {
        meta.bsp_timestamp_check = check_set_bsp_timestamp.execute((bit<16>)meta.bsp_task_id);
        // meta.bsp_timestamp = hdr.ping_payload.task_start_tstamp[31:0];
    }
    @stage(0) table check_set_bsp_timestamp_t {
        actions = { check_set_bsp_timestamp_a; }
        default_action = check_set_bsp_timestamp_a;
    }
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_timestamp) get_bsp_timestamp = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            result = register_data;
        }
    };
    action get_bsp_timestamp_a() {
        meta.bsp_timestamp = get_bsp_timestamp.execute((bit<16>)meta.bsp_task_id);

        meta.bsp_timeout.timeout_time = (bit<32>)ig_intr_md.ingress_mac_tstamp[46:16];
        //hdr.my_pong_wait_ports.setValid();
        // hdr.ipv4.setValid();
        // hdr.udp.setValid();
        // hdr.pong_payload.setValid();
    }
    @stage(0) table get_bsp_timestamp_t {
        actions = { get_bsp_timestamp_a; }
        default_action = get_bsp_timestamp_a;
    }


// 2. BSP waiting-port operations.
    // Initialize to zero.
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_waiting_ports) reset_bsp_waiting_ports = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            // For a new task, set the bitmap for the current port; for the
            // same task, OR it into the existing bitmap.
            if(meta.bsp_timestamp_check == 2){
                register_data = meta.bsp_waiting_ports;
            }
            else if(meta.bsp_timestamp_check == 1){
                register_data = register_data|meta.bsp_waiting_ports;
            }
            result = register_data;
        }
    };
    action reset_bsp_waiting_ports_a() {
        meta.bsp_waiting_ports = reset_bsp_waiting_ports.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(1) table reset_bsp_waiting_ports_t {
        actions = { reset_bsp_waiting_ports_a; }
        default_action = reset_bsp_waiting_ports_a;
    }

// 3. BSP forwarding-port operations.
    // Read the value to test whether it is zero.
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_forwarding_ports) get_bsp_forwarding_ports = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // set
                register_data = meta.bsp_forwarding_ports;
            }
            else { // Clear the PONG packet by XOR.
                register_data = register_data ^ meta.bsp_forwarding_ports;
            }
            result = register_data; // Read the value; zero means a PONG is needed.
            
        }
    };
    action get_bsp_forwarding_ports_a() {
        meta.bsp_forwarding_ports = get_bsp_forwarding_ports.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(2) table reset_bsp_forwarding_ports_t {
        actions = { get_bsp_forwarding_ports_a; }
        default_action = get_bsp_forwarding_ports_a;
    }






// 4. BSP path-count operations.
    RegisterAction<bit<16>, bit<16>,bit<16>>(bsp_path_count) reset_bsp_path_count = {
        void apply(inout bit<16> register_data, out bit<16> result) {
            if((bit<16>)meta.bsp_timestamp_check == 2){ // set
                register_data = 0;
            }
            else{ // Add the PONG packet path count.
                register_data = register_data + meta.bsp_path_count;
            }
            result = register_data; // Return current value.
        }
    };
    action reset_bsp_path_count_a() {
        meta.bsp_path_count = reset_bsp_path_count.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(2) table reset_bsp_path_count_t {
        actions = { reset_bsp_path_count_a; }
        default_action = reset_bsp_path_count_a;
    }

// 5. BSP maximum-delay operations.
    // Check and compare.
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_max_delay) check_bsp_max_delay = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            // For a new task or a larger value, update directly.
            if(meta.bsp_timestamp_check == 2||meta.bsp_max_delay > register_data){
                register_data = meta.bsp_max_delay;
            }
            else{
                result = register_data;
            }
        }
    };
    action check_bsp_max_delay_a(){
        meta.bsp_max_delay_check = check_bsp_max_delay.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(2) table set_bsp_max_delay_t {
        actions = { check_bsp_max_delay_a; }
        default_action = check_bsp_max_delay_a;
    }

// 6. BSP maximum-delay-link operations.
    // Read the value.
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_max_delay_link) get_bsp_max_delay_link = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_max_delay_check == 0){ // set update
                register_data = meta.bsp_max_delay_link;
            }
            // Otherwise leave the register unchanged and return it.
            result = register_data;
        }
    };
    action get_bsp_max_delay_link_a() {
        meta.bsp_max_delay_link = get_bsp_max_delay_link.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(3) table set_bsp_max_delay_link_t {
        actions = { get_bsp_max_delay_link_a; }
        default_action = get_bsp_max_delay_link_a;
    }
    
// 7. BSP drop-link operations: update and read back.
    // Read and update.
    RegisterAction<bit<32>, bit<16>,bit<32>>(bsp_drop_link) get_bsp_drop_link = {
        void apply(inout bit<32> register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // New task, set the value.
                register_data = 0;
            }
            else if(meta.bsp_drop_link!=0){ // Update when the PONG packet carries a drop link.
                register_data = meta.bsp_drop_link;
            }
            result = register_data;
        }
    };
    action get_bsp_drop_link_a() {
        meta.bsp_drop_link = get_bsp_drop_link.execute((bit<16>)meta.bsp_task_id);
    }
    // @stage(5) table get_bsp_drop_link_t {
    //     actions = { get_bsp_drop_link_a; }
    //     default_action = get_bsp_drop_link_a;
    // }
    @stage(2) table reset_bsp_drop_link_t {
        actions = { get_bsp_drop_link_a; }
        default_action = get_bsp_drop_link_a;
    }

// 8. BSP timeout-time operations: compare time and return timeout state.
    // Set time.
    RegisterAction<bsp_timeout_pair_h, bit<16>,bit<32>>(bsp_timeout_flag_time) set_bsp_timeout_flag_time = {
        void apply(inout bsp_timeout_pair_h register_data, out bit<32> result) {
            if(meta.bsp_timestamp_check == 2){ // New task, reset to zero.
                register_data.timeout_time = meta.bsp_timeout.timeout_time;
                register_data.timeout_flag = 0;
            }
            // else if(register_data.timeout_time < meta.bsp_timeout.timeout_time){
            //     register_data.timeout_flag = 1;
            // }
            result = register_data.timeout_flag;
        }
    };
    action set_bsp_timeout_flag_time_a() {
        meta.bsp_timeout.timeout_flag = set_bsp_timeout_flag_time.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(1) table set_bsp_timeout_flag_time_t {
        actions = { set_bsp_timeout_flag_time_a; }
        default_action = set_bsp_timeout_flag_time_a;
    }
    RegisterAction<bsp_timeout_pair_h, bit<16>,bit<32>>(bsp_timeout_flag_time) check_bsp_timeout_flag_time = {
        void apply(inout bsp_timeout_pair_h register_data, out bit<32> result) {

            if (register_data.timeout_time < meta.bsp_timeout.timeout_time && register_data.timeout_flag == 0)
            {
                result = 0;
            }
            else 
            {
                result = 1;
            }
            if(register_data.timeout_time < meta.bsp_timeout.timeout_time){ // Previously not timed out, now timed out.
                register_data.timeout_flag = 1;
                
            }
        }
    };

    action check_bsp_timeout_flag_time_a() {
        meta.bsp_timeout.timeout_flag = check_bsp_timeout_flag_time.execute((bit<16>)meta.bsp_task_id);
    }
    @stage(1) table check_bsp_timeout_flag_time_t {
        actions = { check_bsp_timeout_flag_time_a; }
        default_action = check_bsp_timeout_flag_time_a;
    }
    // @stage(1) table get_bsp_timeout_flag_time_t {
    //     actions = { set_bsp_timeout_flag_time_a; }
    //     default_action = set_bsp_timeout_flag_time_a;
    // }

    action push_back_timeout_a(bit<12> port){
        
        hdr.ethernet.ether_type = ether_type_t.IPV4;

        hdr.ipv4.src_addr = meta.bsp_dst_ip;
        hdr.ipv4.dst_addr = meta.bsp_src_ip;
        hdr.ipv4.protocol = 100; // PONG packet marker.

        hdr.udp.src_port = meta.bsp_sp_dp[15:0];
        hdr.udp.dst_port = meta.bsp_sp_dp[31:16];

        hdr.pong_payload.task_ID = meta.bsp_task_id;
        hdr.pong_payload.task_start_tstamp = (bit<32>)meta.bsp_timestamp;
        hdr.pong_payload.path_count =  (bit<16>)meta.bsp_path_count;
        hdr.pong_payload.max_delay = meta.bsp_max_delay;
        hdr.pong_payload.max_delay_link = meta.bsp_max_delay_link;
        hdr.pong_payload.drop_link = SWITCH_ID | (bit<32>)port;

        // FIRST_PONG loopback-broadcast headers.
        hdr.my_pong_wait_ports.wait_ports = meta.bsp_waiting_ports;
        hdr.ethernet_2.ether_type = ether_type_t.FIRST_PONG;
        hdr.my_pong_wait_ports.setValid();
        hdr.ethernet_2.setValid();
        ig_tm_md.ucast_egress_port=CIRC_PORT_1;
    }
    @stage(4) table push_back_timeout_t {
        key={meta.bsp_forwarding_ports:ternary;}
        actions = { push_back_timeout_a; }
        default_action = push_back_timeout_a(0);
    }
    action push_back_ping_pong_a(){
        hdr.pong_payload.task_ID = meta.bsp_task_id;
        hdr.pong_payload.task_start_tstamp = (bit<32>)meta.bsp_timestamp;
        hdr.pong_payload.path_count =  (bit<16>)meta.bsp_path_count;
        hdr.pong_payload.max_delay = meta.bsp_max_delay;
        hdr.pong_payload.max_delay_link = meta.bsp_max_delay_link;
        hdr.pong_payload.drop_link = meta.bsp_drop_link;
        hdr.udp.src_port = meta.dst_port;
        hdr.udp.dst_port = meta.src_port;
        hdr.ipv4.src_addr = meta.dst_addr;
        hdr.ipv4.dst_addr = meta.src_addr;
        hdr.ipv4.protocol = 100; // PONG broadcast loopback packet.
        ig_dprsr_md.drop_ctl = 0;
        hdr.pong_payload.setValid();
        hdr.ping_payload.setInvalid(); // Invalidate ping_payload.
        ig_tm_md.ucast_egress_port = ig_intr_md.ingress_port;
    }
    @stage(4) table push_back_ping_pong_t {
        actions = { push_back_ping_pong_a; }
        default_action = push_back_ping_pong_a;
    }
    action push_back_in_pong(){
        hdr.pong_payload.path_count =  meta.bsp_path_count;
        hdr.pong_payload.max_delay = meta.bsp_max_delay;
        hdr.pong_payload.max_delay_link = meta.bsp_max_delay_link;
        hdr.pong_payload.drop_link = meta.bsp_drop_link;


        // FIRST_PONG loopback-broadcast headers.
        hdr.my_pong_wait_ports.wait_ports = meta.bsp_waiting_ports;
        hdr.ethernet_2.ether_type = ether_type_t.FIRST_PONG;
        hdr.my_pong_wait_ports.setValid();
        hdr.ethernet_2.setValid();
        ig_tm_md.ucast_egress_port=CIRC_PORT_1; // Dedicated loopback port for PONG broadcast.
    }
    @stage(4) table push_back_in_pong_t {
        actions = { push_back_in_pong; }
        default_action = push_back_in_pong;
    }


    Register<bit<1>, bit<1>>(2, 0) reg_circ_pkt_gen;
    RegisterAction<bit<1>, bit<1>,bit<1>>(reg_circ_pkt_gen) circ_pkt_gen = {
        void apply(inout bit<1> register_data, out bit<1> result) {
            result = register_data;
            register_data = 1;
        }
    };
    action gen_circ_pkt_a() {
        meta.have_circled =circ_pkt_gen.execute(0);
        
    }
    @stage(5) table gen_circ_pkt_t {
        actions = { gen_circ_pkt_a; }
        default_action = gen_circ_pkt_a;
    }

    action mirror_circ_pkt_a() {
            meta.circ_mirror.ether_type = ether_type_t.CIRC;
            ig_dprsr_md.mirror_type = 2;
            meta.session_id = 2;
    }
    @stage(6) table mirror_circ_pkt_t {
        actions = { mirror_circ_pkt_a; }
        default_action = mirror_circ_pkt_a;
    }
    
    Register<bit<32>, bit<16>>(BSP_SIZE, 0) bsp_timestamp_sent; // Stage ?, uses one register action.

    RegisterAction<bit<32>, bit<16>,bit<1>>(bsp_timestamp_sent) check_latest_timestamp = {
        void apply(inout bit<32> register_data, out bit<1> result) {
            if(meta.bsp_timestamp > register_data){
                result = 1;// newer timestamp
                register_data = meta.bsp_timestamp;
            }
            else{
                result = 0;// older timestamp
            }
        }
    };
/*
Registers maintained per task:
key(index): task_ID.
State to maintain:
bit<32> Task start timestamp, using the lower 32 bits.
bit<1>  Timeout flag, indicating whether the current task is available.
bit<32> Timeout timestamp, using the upper 32 bits.
bit<32> Waiting-port bitmap. Each port maps to one bitmap bit through a table;
        the bit is set when the packet arrives.
bit<32> Forwarding-port bitmap. Initialized from the route-table port; cleared
        when the corresponding PONG packet arrives; when all bits are zero,
        aggregate and return the PONG packet.
bit<16> Path count, initialized to zero and accumulated from PONG packets.
bit<32> Maximum end-to-end path delay. PING carries the previous-hop delay, and
        PONG aggregates the downstream links, in ns.
bit<32> Drop-link information: 20-bit switch ID + 12-bit port ID. On task
        timeout, return any link by taking the lowbit of the forwarding-port
        bitmap and looking up the corresponding port.
lowbit: x & ((~x)+1), which returns the least significant set bit.
bit<32> Maximum-delay link information. Maintained together with the maximum
        end-to-end path delay, as 20-bit switch ID + 12-bit port ID.

When a PONG packet returns, loop back to enumerate every bit in the waiting-port
bitmap. If a bit is set, send a PONG packet. The loop can start from 1 and shift
left each time until it becomes zero.
*/
apply {
/*
*/
    if(hdr.arp.isValid()) {
        arp_host.apply();
    }
    else if((hdr.ping_payload.isValid()||hdr.pong_payload.isValid())||meta.bsp_is_circ == 1){
        /* Prefer ingress_port when checking first arrival and reset circ_times. */
        //stage:: 0
        cal_ecmp_t.apply(); // Compute the ECMP path for loopback packets.
        if(meta.bsp_is_circ == 1){
            get_bsp_timestamp_t.apply();
        }
        else{
            if(meta.bsp_circ_times==0){
                // Initialize the forwarding-port bitmap; all possible port bits are set to 1.
                ipv4_host_ping_t.apply();
                // Compute timeout time.
                cal_timeout_time_t.apply();
                ingress_port_to_bit_waiting_t.apply();
            }
            else if (meta.bsp_is_pong == 1){  // Do not read forwarding for recirculated ping packets.
                ingress_port_to_bit_forward_t.apply();
            }
            check_set_bsp_timestamp_t.apply();
        }
        //over
        //stage:: 1
        // hdr.ping_payload.circ_times>0||(meta.bsp_is_pong == 1&&meta.bsp_timestamp_check!=1)
        // circ_times can be moved into meta and initialized here to avoid too many condition checks.
        if((meta.bsp_is_circ == 1||meta.bsp_circ_times==0)||(meta.bsp_is_pong == 1 && meta.bsp_timestamp_check ==1))
        {
            // Set timeout result. New task: reset to 0.
            // Current task/PONG/CIRC: check and read the timeout status.
            if(meta.bsp_is_circ == 1){
                check_bsp_timeout_flag_time_t.apply();
            }
            else{
                set_bsp_timeout_flag_time_t.apply();
            }
            // bit<32> waiting-port bitmap. A table maps each port to one bitmap
            // bit, which is set when the packet arrives.
            // New task: assign. Current task: OR into the existing bitmap.
            // PONG/CIRC: read waiting-port bitmap.
            reset_bsp_waiting_ports_t.apply(); // ingress_port -> bit must be updated first. // stage2
        }
        ipv4_host_t.apply();
        if(hdr.ping_payload.isValid()){
            if((meta.bsp_circ_times==0 && meta.bsp_timestamp_check[1:0] != 2)||hdr.ping_payload.next_hop_bit==0||meta.bsp_timeout.timeout_flag[0:0] == 1){
                drop();
            }
        }
        if(meta.bsp_timestamp_check ==2){
            ping_forwarding_ports_check_t.apply(); // meta.bsp_forwarding_ports = hdr.ping_payload.next_hop_bit;
        }
        
        // At this point, bsp_is_pong can identify PONG packets for the current task.
        //stage:: 2
        // Timestamp 1 also reads content; values should stay 0, and
        // loopback packets should not enter.
        if(meta.bsp_timeout.timeout_flag[0:0] == 0 &&((meta.bsp_timestamp_check[1:0] != 0 && meta.bsp_circ_times==0)||(meta.bsp_is_pong == 1||meta.bsp_is_circ == 1))){
            // bit<32> forwarding-port bitmap: configured forwarding ports; PONG clears by XOR.
            reset_bsp_forwarding_ports_t.apply(); // Initialize route-table ports.
            // bit<16> path count: starts at 0 and accumulates PONG packets.
            reset_bsp_path_count_t.apply();
        }
        // Only timestamp 1 should be changed.
        if(meta.bsp_timeout.timeout_flag[0:0] == 0 &&((meta.bsp_timestamp_check[1:0] != 0 && meta.bsp_circ_times==0)||(meta.bsp_is_pong == 1||meta.bsp_is_circ == 1))){
            // bit<32> forwarding-port bitmap: configured forwarding ports; PONG clears by XOR.
            // reset_bsp_forwarding_ports_t.apply(); // Initialize route-table ports.
            // // bit<16> path count: starts at 0 and accumulates PONG packets.
            // reset_bsp_path_count_t.apply();
            // bit<32> maximum end-to-end path delay. PING carries the previous-hop
            // delay, and PONG aggregates the downstream links, in ns.
            set_bsp_max_delay_t.apply(); // Initialize.
            // // bit<32> drop-link information: 20-bit switch ID + 12-bit port ID.
            // // On timeout, return any link by taking the lowbit of the
            // // forwarding-port bitmap and looking up the corresponding port.
            reset_bsp_drop_link_t.apply();
        }
        if(hdr.ping_payload.isValid()){
            if(meta.next_port_bit != 0){
                pkt_send_to_port_t.apply(); // Forward ping.
            }
            else{
                pkt_send_to_circ_t.apply(); // Loop ping back.
            }
        }

        //stage:: 3
        // Similarly, timestamp 0 should not enter.
        if(meta.bsp_timeout.timeout_flag[0:0] == 0 &&((meta.bsp_timestamp_check[1:0] != 0 && meta.bsp_circ_times==0)||(meta.bsp_is_pong == 1||meta.bsp_is_circ == 1))){
            set_bsp_max_delay_link_t.apply(); // Initialize; stage 2 is insufficient because of stage limits.
            set_bsp_src_ip_t.apply();
            set_bsp_dst_ip_t.apply();
            set_bsp_src_dst_port_t.apply();
        }
        if(meta.bsp_max_delay_check != 0){ // Move staged value.
            meta.bsp_max_delay = meta.bsp_max_delay_check;
        }
        
        if(meta.next_port_bit != 0 && hdr.ping_payload.next_hop_bit!=0){
            if(meta.bsp_timestamp_check==2||hdr.ping_payload.circ_times>0){
                circle_ping_t.apply();
            }
        }
        //stage:: 4
        // Simulate loopback broadcast.
        if(meta.bsp_forwarding_ports == 0 && meta.bsp_timeout.timeout_flag[0:0] == 0){
            if (meta.bsp_is_pong == 1)
            {
                push_back_in_pong_t.apply();
            }
            else if(meta.bsp_circ_times[11:0] == 0 && meta.bsp_timestamp_check[0:0] ==1)
                    push_back_ping_pong_t.apply();
            set_port.execute(meta.bsp_task_id);
            set_final_bits.execute(meta.bsp_task_id);
        }
        else if(meta.bsp_is_circ == 1 && meta.bsp_timeout.timeout_flag[0:0] == 0){
            push_back_timeout_t.apply();
        }
        //stage:: 5 
        // circle timeout check
        if(meta.bsp_is_pong == 1){
            gen_circ_pkt_t.apply();
        }
        //stage:: 6
        if(meta.bsp_is_circ == 1||(meta.bsp_is_pong == 1&&meta.have_circled == 0)){
            mirror_circ_pkt_t.apply();
        }
    }
}
}

control IngressDeparser(packet_out pkt,
    /* User */
    inout my_ingress_headers_t                       hdr,
    in    my_ingress_metadata_t                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md)
{
        // Checksum() ipv4_checksum;
    
    
    Checksum() ipv4_checksum;
    Mirror() mirror;
    apply {
        // if (hdr.ipv4.isValid()) {
        //     hdr.ipv4.hdr_checksum = ipv4_checksum.update({
        //         hdr.ipv4.version,
        //         hdr.ipv4.ihl,
        //         hdr.ipv4.diffserv,
        //         hdr.ipv4.res,
        //         hdr.ipv4.total_len,
        //         hdr.ipv4.identification,
        //         hdr.ipv4.flags,
        //         hdr.ipv4.frag_offset,
        //         hdr.ipv4.ttl,
        //         hdr.ipv4.protocol,
        //         hdr.ipv4.src_addr,
        //         hdr.ipv4.dst_addr
        //     });  
        // }
        if (ig_dprsr_md.mirror_type == 1){
            mirror.emit<ping_mirror_h>(meta.session_id, meta.ping_mirror);
            // mirror.emit(meta.session_id);
        }
        else if (ig_dprsr_md.mirror_type == 2)
            mirror.emit<circ_mirror_h>(meta.session_id, meta.circ_mirror);
        pkt.emit(hdr);
    }
}
/*************************************************************************
 ****************  E G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/



/*
Loopback packets:
1. All loopback packets are handled by mirror. Do not explicitly specify the
   loopback port.
2. Loopback packet types:
   ping type: carries ping_payload, uses the same type as ping, port 191.
       Simulates broadcast.
       A simulated broadcast packet may leave through two paths: one loopback
       port and one normal port.
       Loopback port: use mirror.
       Normal port: forward through the lookup table and normal pipeline.
       Conflict: the switch loopback-count field must be incremented for the
       loopback packet and reset to 0 for the forwarded packet.
       Resolution: in egress, inspect the port. If the packet is forwarded,
       reset its loopback count to 0 in the corresponding header field.
       Egress must parse packets down to the ping/pong payload level.

   pong type: carries pong_payload, newly defined type, ports 192 and 193.

       task_ID += 1    stage 0
       Check timeout.
       Set ipv4.protocol to 101, which differs from both ping and pong.
       task_ID: current task being scanned.
       The timeout flag is updated only when a pong-type loopback packet
       arrives.

       The question is whether one pass through the switch needs two register
       accesses. The planned flow is:
           Force a comparison to decide timeout. If not timed out (0), ignore.
           Timeout: access the flag register in stage 2.
               Previously not timed out: access returns 0, enter timeout path.
               Previously timed out: access returns 1, ignore.
       Some fields are reused here and placed in pong:
           task_ID, used to iterate over packets.

       Three stages are needed.

       Timeout handling:
       ingress:
       stage 3:
       Fill pong_payload in one stage.
       8. task ID, 2 bytes (reused from the PING packet)
           No action needed.
       9. Task start timestamp, 6 bytes (reused from the PING packet)
           Read directly.
       10. Hop count, 1 byte (reused from PING)
           Read directly.
       11. Path count, 2 bytes (aggregated)
           Read directly.
       12. Maximum end-to-end path delay, 4 bytes (aggregated)
           Read directly.
       13. Maximum-delay link information, 4 bytes (aggregated)
           Read directly.
       14. Drop-link information, 4 bytes (aggregated)
           Read first, trying to reuse drop information collected by other
           switches.
           If missing:
               Read the forwarding-port bitmap. It must be nonzero. 1 stage.

               stage 4-5:
                   a = 0 - x   1-2 stages
                       a = ~x
                       a = a + 1
               stage 6:
                   a = a & x   1 stage

               stage 7:
               After extracting the value, look it up in a table. 1 stage.

               stage 8: this may be handled earlier if the high and low bits
               are independent.
               Fill the high bits with SWITCH_ID. 1 stage.

               Use the lowbit operation to select the lowest unforwarded link.

       Mirror one packet to loopback.

       egress:
       The protocol must be changed to pong type. Implement this in egress: the
       egress parser parses the packet, and if the port is not loopback, mark it
       as pong type.
*/

/*
Egress tasks:

Ping loopback:
Update circle_times to distinguish loopback packets from forwarded packets.
   Loopback packet: +1.
   Forwarded packet: set to 0.
Generate loopback packet:
   Use a 1-bit register to record whether the loopback packet has been
   generated.
   If it has not been generated, mirror one IPv4-based packet into the loopback
   port. Change the IPv4 protocol number to a custom protocol and add matching
   parse logic in the ingress parser.
   Egress must parse the packet as IPv4.

   Then further distinguish ping and pong by port, and record link delay and
   link information.
*/

    /***********************  H E A D E R S  ************************/


struct my_egress_headers_t {
    ethernet_h         ethernet2;
    next_hop_bit_h     next_hop_bit;
    my_pong_wait_ports_h    my_pong_wait_ports;
    ethernet_h         ethernet;
    ipv4_h             ipv4;
    udp_h              udp;
    ping_payload_h     ping_payload;
    pong_payload_h     pong_payload;
    
}


    /********  G L O B A L   E G R E S S   M E T A D A T A  *********/

struct my_egress_metadata_t {
    ethernet_h ethernet2;
    circ_t circ;
    MirrorId_t session_id;//bit<10>
    pong_mirror_h pong_mirror;

}

    /***********************  P A R S E R  **************************/

parser EgressParser(packet_in        pkt,
    /* User */
    out my_egress_headers_t          hdr,
    out my_egress_metadata_t         meta,
    /* Intrinsic */
    out egress_intrinsic_metadata_t  eg_intr_md)
{
    /* This is a mandatory state, required by Tofino Architecture */
    state start {
        pkt.extract(eg_intr_md);
        // pkt.advance(PORT_METADATA_SIZE);
        transition parse_ethernet2;
    }
    
    state parse_ethernet2 {
        pkt.extract(hdr.ethernet2);
        transition select((bit<16>)hdr.ethernet2.ether_type) {
            (bit<16>)ether_type_t.IPV4            :  parse_ipv4;
            (bit<16>)ether_type_t.HBIT            :  parse_next_hop_bit;
            (bit<16>)ether_type_t.CIRC            :  parse_ethernet;
            (bit<16>)ether_type_t.PONG            :  parse_waiting_port;
            (bit<16>)ether_type_t.FIRST_PONG      :  parse_waiting_port;
            default :  accept;
        }
    }
    state parse_waiting_port{
        pkt.extract(hdr.my_pong_wait_ports);
        transition parse_ethernet;
    }
    state parse_next_hop_bit {
        pkt.extract(hdr.next_hop_bit);
        transition parse_ethernet;
    }
    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select((bit<16>)hdr.ethernet.ether_type) {
            (bit<16>)ether_type_t.IPV4            :  parse_ipv4;
            default :  accept;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
           17 : parse_udp;
           
           16 : parse_udp;// 
          100 : parse_udp;//
          101 : parse_udp;//
            default : accept;
        }
    }
    state parse_udp {
        pkt.extract(hdr.udp);
        transition select(hdr.ipv4.protocol) {           
           16 : parse_ping_payload;// 
          100 : parse_pong_payload;//
          101 : parse_pong_payload;//
            default : accept;
        }
    }
    state parse_ping_payload{
        pkt.extract(hdr.ping_payload);
        transition accept;
    }
    state parse_pong_payload{
        pkt.extract(hdr.pong_payload);
        transition accept;
    }
}

    /***************** M A T C H - A C T I O N  *********************/

control Egress(
    /* User */
    inout my_egress_headers_t                          hdr,
    inout my_egress_metadata_t                         meta,
    /* Intrinsic */    
    in    egress_intrinsic_metadata_t                  eg_intr_md,
    in    egress_intrinsic_metadata_from_parser_t      eg_prsr_md,
    inout egress_intrinsic_metadata_for_deparser_t     eg_dprsr_md,
    inout egress_intrinsic_metadata_for_output_port_t  eg_oport_md)
{
    
    Register<bit<1>, bit<1>>(2, 0) reg_circ_pkt_gen;
    RegisterAction<bit<1>, bit<1>,bit<1>>(reg_circ_pkt_gen) circ_pkt_gen = {
        void apply(inout bit<1> register_data, out bit<1> result) {
            result = register_data;
            register_data = 1;
        }
    };
    action gen_circ_pkt_a() {
        meta.circ.task_ID = 0;
        meta.circ.wait_ports = 0;
    }
    action drop() {
        eg_dprsr_md.drop_ctl = 1; // Drop packet.
    }
    action get_high_bit_a(bit<32> high_bit,MirrorId_t session_id)
    {
        meta.pong_mirror.wait_ports = hdr.my_pong_wait_ports.wait_ports^high_bit; // Clear the highest bit.
        meta.pong_mirror.ether_type = ether_type_t.PONG;
        eg_dprsr_md.mirror_type = 1;
        meta.session_id = session_id;
        //meta.ethernet2.ether_type = ether_type_t.PONG;
    }
    @stage(0)  table get_high_bit_t // 32-bit bitmap -> highest bit and corresponding session_id.
    {
        key={hdr.my_pong_wait_ports.wait_ports:ternary;}
        actions={get_high_bit_a;}
        default_action=get_high_bit_a(0,0);
        size=100;
    }
apply {
    if(hdr.ping_payload.isValid()){
        if(eg_intr_md.egress_port != CIRC_PORT_1){ // Forward ping-type packet.
            hdr.ping_payload.circ_times = 0;
            hdr.ping_payload.last_hop_delay = (bit<32>)eg_intr_md.deq_timedelta;
            hdr.ping_payload.last_hop_link[8:0] = eg_intr_md.egress_port;
        }
        // Ping loopback broadcast. Increment circ_times because ingress mirror
        // cannot update it; mirror and forwarding are normalized here.
        else{
            hdr.ping_payload.circ_times = hdr.ping_payload.circ_times + 1;
            if(hdr.next_hop_bit.isValid()){
                // Forwarded packet.
                hdr.ping_payload.next_hop_bit = hdr.next_hop_bit.bitval;
                hdr.next_hop_bit.setInvalid();
                hdr.ethernet2.setInvalid();
            }
        }
    }
    else if(hdr.ethernet2.ether_type == ether_type_t.CIRC)
    {
        hdr.ethernet2.setInvalid();
        hdr.pong_payload.task_ID = hdr.pong_payload.task_ID + 1;
        hdr.ipv4.protocol = 101;
        if (hdr.pong_payload.task_ID == BSP_SIZE)
        hdr.pong_payload.task_ID = 0;
        
    }
    else if(hdr.pong_payload.isValid()){ // Loop pong_send-type packets until the bitmap reaches zero.
        if (hdr.my_pong_wait_ports.wait_ports != 0)
        {
            get_high_bit_t.apply();
        }
        if (hdr.ethernet2.ether_type == ether_type_t.FIRST_PONG)
        {
            drop();
        }
        if (hdr.my_pong_wait_ports.isValid())
        {
            hdr.ethernet2.setInvalid();
            hdr.my_pong_wait_ports.setInvalid();
        }
    }
}
}



    /*********************  D E P A R S E R  ************************/

control EgressDeparser(packet_out pkt,
    /* User */
    inout my_egress_headers_t                       hdr,
    in    my_egress_metadata_t                      meta,
    /* Intrinsic */
    in    egress_intrinsic_metadata_for_deparser_t  eg_dprsr_md)
{
    Mirror() mirror;
    
    apply {
        if (eg_dprsr_md.mirror_type == 1){
            mirror.emit<pong_mirror_h>(meta.session_id, meta.pong_mirror);
        }
        pkt.emit(hdr);
    }
}


/************ F I N A L   P A C K A G E ******************************/
Pipeline(
    IngressParser(),
    Ingress(),
    IngressDeparser(),
    EgressParser(),
    Egress(),
    EgressDeparser()
) pipe;

Switch(pipe) main;
