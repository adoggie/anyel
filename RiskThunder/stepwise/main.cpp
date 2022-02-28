#include <iostream>

#include "messagebuilder.h"
#include "traverse-message.h"


std::string MessageSendOrderRequest_(){
    auto m = std::make_shared<MessageSendOrderRequest>();
    m->network_->from_id ="thunder-01";
    m->network_->from_service ="thunder";
    m->network_->dest_id ="rrs001";
    m->network_->dest_service ="rulerisk_server";
    m->network_->signature = "nosig";
    m->account ="abc001";
    m->refno = "10001";
    m->hyname ="RB01";
    m->num = 11;
    m->direction = DIRECTION_BUY;
    m->openclose = OC_OPEN;
    m->price = 900.0021;
    auto bytes = MessageBuilder::marshall( m );
    std::string text ;
    text.assign(bytes.begin(),bytes.end());
    return text;
}


std::string MessageOnTrade_(){
    auto m = std::make_shared<MessageOnTrade>();
    m->network_->from_id ="thunder-01";
    m->network_->from_service ="thunder";
    m->network_->dest_id ="rrs001";
    m->network_->dest_service ="rulerisk_server";
    m->network_->signature = "nosig";
    m->account ="abc001";
    m->refno = "10001";
    m->hyname ="RB01";
    m->tradenum = 11;
    m->direction = DIRECTION_BUY;
    m->openclose = OC_OPEN;
    auto bytes = MessageBuilder::marshall( m );
    std::string text ;
    text.assign(bytes.begin(),bytes.end());
    return text;
}

std::string MessageThunderStatus_(){
    auto m = std::make_shared<MessageThunderStatus>();
    m->network_->from_id ="thunder-01";
    m->network_->from_service ="thunder";
    m->network_->dest_id ="rrs001";
    m->network_->dest_service ="rulerisk_server";
    m->network_->signature = "nosig";
    m->account ="abc001";
    m->status = "10001";
    m->date =20201101;
    m->time = 11;

    auto bytes = MessageBuilder::marshall( m );
    std::string text ;
    text.assign(bytes.begin(),bytes.end());
    return text;
}

std::string MessageInfoHY_(){
    auto m = std::make_shared<MessageInfoHY>();
    m->network_->from_id ="thunder-01";
    m->network_->from_service ="thunder";
    m->network_->dest_id ="rrs001";
    m->network_->dest_service ="rulerisk_server";
    m->network_->signature = "nosig";
    m->account ="abc001";
    m->hyname = "hyname";
    m->cancelnum =11;
    m->tlong = 12;
    m->ylong = 13;
    m->tshort = 14;
    m->yshort = 15;
    m->opennum = 16;

    auto bytes = MessageBuilder::marshall( m );
    std::string text ;
    text.assign(bytes.begin(),bytes.end());
    return text;
}

std::string MessageInfoOrder_(){
    auto m = std::make_shared<MessageInfoOrder>();
    m->network_->from_id ="thunder-01";
    m->network_->from_service ="thunder";
    m->network_->dest_id ="rrs001";
    m->network_->dest_service ="rulerisk_server";
    m->network_->signature = "nosig";
    m->account ="abc001";
    m->hyname = "hyname";
    m->refno ="11";
    m->num = 12;
    m->direction = DIRECTION_BUY;
    m->openclose = OC_OPEN;
    m->price = 15;
    m->status = "b";

    auto bytes = MessageBuilder::marshall( m );
    std::string text ;
    text.assign(bytes.begin(),bytes.end());
    return text;
}


int main() {
    std::cout << "Hello!" << std::endl;
    MessageBuilder::instance().initialize("aa","bb");
    auto text = MessageSendOrderRequest_();
    text = MessageOnTrade_();
    text = MessageThunderStatus_();
    text = MessageInfoHY_();
    text = MessageInfoOrder_();
    std::cout << text << std::endl;

    text ="1.0,rulerisk_server:,SEND_ORDER_RESP,:,1645584184,nosig,base64,eyJhY2NvdW50IjogImFjdDAwMSIsICJyc3QiOiAxLCAibXNnIjogImFhYVx1MjAxNFx1MjAxNGJiYiIsICJyZWZubyI6ICIwMDEifQ==";
    auto m = MessageBuilder::unmarshall(ByteArray(text.begin(),text.end()));
    if(m){
        MessageSendOrderResponse::Ptr resp = std::dynamic_pointer_cast<MessageSendOrderResponse>(m);
        if(resp) {
            std::cout << m->name_ << std::endl;
        }
    }
    return 0;
}


