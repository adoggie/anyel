
#include <time.h>
#include "network-message.h"
#include "traverse-message.h"
#include "base64.h"

//namespace elabs{
    ByteArray b64_decode(const std::string& text){
        return Base64::decode(text);
    }

    std::string b64_encode(const ByteArray& bytes){
        return Base64::encode(bytes);
    }

    NetworkMessage::NetworkMessage(){
        time_t ts = time(NULL);
        ver = "1.0";
        dest_service ="riskrule_server";
        dest_id = "";
        from_service ="thunder";
        from_id = "";
		timestamp = (long long )ts;
        encode ="base64";
    }

    ByteArray NetworkMessage::marshall(const std::shared_ptr<TraverseMessage> &payload, const std::string &encode )  {
        std::stringstream  ss;
        const char SP=',';

		msg_type =  payload->name_;
        ss << ver << SP << dest_service << ":" << dest_id << SP;
        ss << msg_type << SP << from_service << ":" << from_id << SP;
        ss << timestamp << SP << signature << SP << encode << SP;
        std::string text = payload->marshall();
        // base64
        auto b64 = b64_encode(ByteArray( (unsigned char *)text.c_str(),(unsigned char *)text.c_str()+text.size()));
        ss << b64;
        text = ss.str();
        return ByteArray (text.begin(),text.end());

		//return ByteArray();
    }

//}