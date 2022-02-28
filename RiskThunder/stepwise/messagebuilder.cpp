#include "messagebuilder.h"

#include "base64.h"
#include "traverse-message.h"

//namespace elabs{


    std::vector<std::string> split(std::string text, char delim) {
        std::string line;
        std::vector<std::string> vec;
        std::stringstream ss(text);
        while(std::getline(ss, line, delim)) {
            vec.push_back(line);
        }
        return vec;
    }

    TraverseMessage::Ptr MessageBuilder::unmarshall( const ByteArray&  data ){
        std::string text(data.begin(),data.end());
        auto fs  = split(text,',');
        if( fs.size() != 8){
            return TraverseMessage::Ptr();
        }

        NetworkMessage::Ptr msg = std::make_shared<NetworkMessage>();
        msg->ver = fs[0];
        auto ds = split(fs[1],':');
        if(ds.size() >0) {
            msg->dest_service = ds[0];
        }
        if(ds.size() > 1){
            msg->dest_id = ds[1];
        }
        msg->msg_type = fs[2];
        ds = split(fs[3],':');
        if(ds.size() >0) {
            msg->from_service = ds[0];
        }
        if(ds.size() > 1){
            msg->from_id = ds[1];
        }
        msg->timestamp = std::stoull(fs[4]);
        msg->signature = fs[5];
        msg->encode = fs[6];
        auto body = fs[7];
        auto bytes = Base64::decode(fs[7]);

        TraverseMessage::Ptr traverse =  TraverseMessage::parse( msg->msg_type, bytes.data(), bytes.size());
        if(traverse){
            traverse->network_ = msg;
        }

        return traverse;
    }

    int MessageBuilder::initialize(const std::string& app_id, const std::string& secret_key ){
        cfgs_["app_id"] = app_id;
        cfgs_["secret_key"] = secret_key;
        return 0;
    }

    ByteArray MessageBuilder::marshall(const std::shared_ptr<TraverseMessage> & message){
        ByteArray bytes;
       // NetworkMessage::Ptr  net = message->network_;
		auto net = message->network_;
        bytes = net->marshall(message);
        return bytes;
    }

//}