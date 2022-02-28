//
// Created by scott on 2022/2/19.
//

#include "traverse-message.h"
#include <jsoncpp/json/json.h>


//namespace elabs{


//typedef std::function< std::shared_ptr< TraverseMessage> (const std::string& msg_type,const Json::Value& root) > ParseFunc;

typedef  std::shared_ptr< TraverseMessage>(*ParseFunc) (const std::string& msg_type,const Json::Value& root)  ;

    //static std::vector<ParseFunc> parse_func_list1={
    //        &MessageSendOrderResponse::parse,
    //};
	static ParseFunc parse_func_list[] ={
            &MessageSendOrderResponse::parse,
    };

    TraverseMessage::Ptr TraverseMessage::parse(const std::string&  msg_type,const Byte * data,size_t size){
        Json::Reader reader;
        Json::Value root;
        TraverseMessage::Ptr msg;

        if (reader.parse((char*)data, root)){
            //for(auto const& func:parse_func_list){
			for(int n=0; n< sizeof(parse_func_list)/sizeof(ParseFunc) ;n++){
                try {
					auto& func = parse_func_list[n]; 
                    msg = func( msg_type,root);
                    if (msg) {
                        return msg;
                    }
                }catch (std::exception& e){
                    std::cout << "Error:" << "TraverseMessage::parse " << e.what() << std::endl;
                    return msg;
                }
            }
        }
        return msg;
    }

    //////////////////////////////////////////////////////
    std::shared_ptr<MessageSendOrderRequest> MessageSendOrderRequest::parse(const std::string& msg_type,const Json::Value& root){
        return std::shared_ptr<MessageSendOrderRequest>();
    };

    ///////////////////////////////////////////////////
    std::shared_ptr<TraverseMessage> MessageSendOrderResponse::parse(const std::string& msg_type,const Json::Value& root){
        std::shared_ptr<TraverseMessage> result;
        if( msg_type ==  MESSAGE_SEND_ORDER_RESP){
            std::shared_ptr<MessageSendOrderResponse> msg = std::make_shared<MessageSendOrderResponse>();
//            Json::Value values = root["values"];
            msg->name_ = MESSAGE_SEND_ORDER_RESP;
            msg->unmarshall(root);
            result = msg ;
        }
		
		//std::shared_ptr<  MessageSendOrderRequest> m(new MessageSendOrderRequest);
        return result;
    }

    ///////////////////////////////////////////////////


//}

