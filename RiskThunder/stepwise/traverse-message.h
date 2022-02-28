

#ifndef STEPWISE_TRAVERSE_MESSAGE_H
#define STEPWISE_TRAVERSE_MESSAGE_H

#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>

#include <jsoncpp/json/json.h>

#include "basetype.h"
#include "network-message.h"

//namespace elabs{


    struct TraverseMessage{
        typedef std::shared_ptr<TraverseMessage> Ptr;

        std::string name_;
        NetworkMessage::Ptr network_;

        TraverseMessage(){ network_ = std::make_shared<NetworkMessage>();}
        virtual ~TraverseMessage(){ }

        TraverseMessage(const std::string& name){ name_ = name; network_ = std::make_shared<NetworkMessage>(); }

        static TraverseMessage::Ptr parse(const std::string&  msg_type,const Byte * data,size_t size);

        virtual Json::Value values(){ return Json::Value();}

        virtual  std::string marshall(){
            Json::Value values = this->values();
            values["name_"] = name_;
            return values.toStyledString();
        }
        virtual bool unmarshall(const Json::Value& root){ return true;}
    };

    ////////////////////////////////////////////////////////////
    //
    struct MessageSendOrderRequest:TraverseMessage{
        std::string  account;
        std::string  refno;
        std::string  hyname;
        long long num ;
        std::string  direction; // direction
        std::string  openclose;
        double      price ;

        MessageSendOrderRequest():TraverseMessage(MESSAGE_SEND_ORDER_REQ){}

        virtual Json::Value values(){
            Json::Value root ;
            root["account"] = account;
            root["refno"] = refno;
            root["hyname"] = hyname;
            root["num"] = num;
            root["direction"] = direction;
            root["openclose"] = openclose;
            root["price"] = price;
            return root;
        }

        static std::shared_ptr<MessageSendOrderRequest> parse(const std::string& msg_type,const Json::Value& root);
    };

    struct MessageSendOrderResponse:TraverseMessage {
        int    rst ;
//        std::vector<std::string> msg;
        std::string     msg;
        std::string     account;
        std::string     refno;

        MessageSendOrderResponse():TraverseMessage(MESSAGE_SEND_ORDER_RESP){}
        static std::shared_ptr<TraverseMessage> parse(const std::string& msg_type,const Json::Value& root);

        virtual bool unmarshall(const Json::Value& root){
            this->rst = root["rst"].asInt();
            this->msg = root["msg"].asString();
//            Json::Value array = root["msg"];
//            if( array.isArray()){
//                for(Json::ArrayIndex n=0; n< array.size() ; n++){
//                    auto value = array[n].asString();
//                    msg.push_back(value);
//                }
//            }
            this->account = root["account"].asString();
            this->refno = root["refno"].asString();
            return true;
        }
    };

    ///// MessageOnTrade �ϱ��ɽ���¼ ///////////////////////////////////////////
    struct MessageOnTrade:TraverseMessage{
        std::string  account;
        std::string  refno;
        std::string  hyname;
        long long tradenum ;
        std::string  direction; // "buy"  "sell"
        std::string  openclose; //"open" "closet" "closey" "close"

        MessageOnTrade():TraverseMessage(MESSAGE_ON_TRADE){}

        virtual Json::Value values(){
            Json::Value root ;
            root["account"] = account;
            root["refno"] = refno;
            root["hyname"] = hyname;
            root["tradenum"] = tradenum;
            root["direction"] = direction;
            root["openclose"] = openclose;
            return root;
        }
//        static std::shared_ptr<MessageSendOrderRequest> parse(const Json::Value& root);
    };

    ///// MessageThunderStatus thunder ״̬�ϱ� ///////////////////////////////////////////
    struct MessageThunderStatus:TraverseMessage{
        std::string  account;
        std::string  status;    // b'-����thunder   ��h��--����
        int date ;
        int time ;

        MessageThunderStatus():TraverseMessage(MESSAGE_THUNDER_STATUS){}
        virtual Json::Value values(){
            Json::Value root ;
            root["account"] = account;
            root["status"] = status;
            root["date"] = date;
            root["time"] = time;
            return root;
        }
    };

    ///// MessageInfoHY   ///////////////////////////////////////////
    struct MessageInfoHY:TraverseMessage{
        std::string  account;
        std::string  hyname;
        int cancelnum ;
        int tlong;
        int ylong ;
        int tshort ;
        int yshort ;
        int opennum ;

        MessageInfoHY():TraverseMessage(MESSAGE_INFO_HY){}
        virtual Json::Value values(){
            Json::Value root ;
            root["account"] = account;
            root["hyname"] = hyname;
            root["cancelnum"] = cancelnum;
            root["tlong"] = tlong;
            root["ylong"] = ylong;
            root["tshort"] = tshort;
            root["yshort"] = yshort;
            root["opennum"] = opennum;
            return root;
        }
    };

    ///// MessageInfoOrder   ///////////////////////////////////////////
    struct MessageInfoOrder:TraverseMessage{
        std::string  account;
        std::string  refno;
        std::string  hyname;
        int num ;
        std::string direction;
        std::string openclose;
        double price ;
        std::string status ;


        MessageInfoOrder():TraverseMessage(MESSAGE_INFO_ORDER){}
        virtual Json::Value values(){
            Json::Value root ;
            root["account"] = account;
            root["refno"] = refno;
            root["hyname"] = hyname;
            root["num"] = num;
            root["direction"] = direction;
            root["openclose"] = openclose;
            root["price"] = price;
            root["status"] = status;
            return root;
        }
    };
//}


#endif //STEPWISE_APP_MESSAGE_H
