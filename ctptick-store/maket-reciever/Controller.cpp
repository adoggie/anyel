
#include "Controller.h"

#include <numeric>
#include <iterator>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <boost/algorithm/string.hpp>

#include "app.h"
#include "version.h"
#include "market.h"
#include "pubservice.h"
#include "mx.h"
#include "HttpService.h"
#include "RegistryClient.h"

bool Controller::init(const Config& props){
	boot_time_ = std::time(NULL);
	cfgs_ = props;
	timer_ = std::make_shared<boost::asio::steady_timer>(io_service_);
	timer_->expires_after(std::chrono::seconds( check_timer_interval_));
	timer_->async_wait(std::bind(&Controller::workTimedTask, this));
    settings_.tick_json = cfgs_.get_int("ctp.market.tick.json",1);
    settings_.tick_raw = cfgs_.get_int("ctp.market.tick.raw",1);
    settings_.tick_text = cfgs_.get_int("ctp.market.tick.text",1);
    return true;
}

bool Controller::open(){
    Application::instance()->getLogger().debug("Controller::open() ..");

    if(cfgs_.get_int("pub_service.enable",0)){
        MessagePublisher::instance().init(cfgs_);
        MessagePublisher::instance().open();
        this->msg_pub_ = &MessagePublisher::instance();
    }

    if( cfgs_.get_int("http_service.enable",0) ){
        //HttpService::instance()->init(cfgs_);
        //HttpService::instance()->open();
    }

    if( cfgs_.get_int("registry_client.enable",1)){
        RegistryClient::instance().init(cfgs_);
        RegistryClient::instance().open();
    }

    if( cfgs_.get_int("ctp.market.enable",0)) {
        getMarketApi()->init(cfgs_);
        getMarketApi()->start();
    }

    if ( cfgs_.get_int("ctp.trade.enable",0)) {
        getTradeApi()->init(cfgs_);
        getTradeApi()->start();
        //getTradeApi()->join();
    }
    io_service_.run();
    if(cfgs_.get_int("ctp.market.join",0)){
        Application::instance()->getLogger().info("MdApi::join()..");
        getMarketApi()->join();
    }
    if(cfgs_.get_int("ctp.trade.join",0)){
        Application::instance()->getLogger().info("TdApi::join()..");
        getTradeApi()->join();
    }

	return true;
}

void Controller::close(){

}

void Controller::run(){
	io_service_.run();
}

void Controller::resetStatus(){
	std::lock_guard<std::recursive_mutex> lock(rmutex_);
	settings_.login_inited = false;
}

// 定时工作任务
void Controller::workTimedTask(){
	std::lock_guard<std::recursive_mutex> lock(rmutex_);
	timer_->expires_after(std::chrono::seconds( check_timer_interval_));
	timer_->async_wait(std::bind(&Controller::workTimedTask, this));
}

//////////////////////////////////////////////////

// 委托回报
void Controller::onOrderReturn(CThostFtdcOrderField *order) {
    if(behavior_) {
        behavior_->onOrderReturn(order);
    }
    if(msg_pub_) {
        std::string  json_text = trade::marshall((const CThostFtdcOrderField*) order,NULL);
        msg_pub_->publish(TopicOrder,json_text);
    }
}

//void Controller::onOrderRespInsert(CThostFtdcInputOrderField *input, CThostFtdcRspInfoField *resp, int nRequestID, bool bIsLast){
//    if(behavior_) {
//        behavior_->onOrderInsert(input,resp);
//    }
//    if(msg_pub_){
//        std::string json_text = trade::marshall(input,resp);
//        msg_pub_->publish(TopicOrderError,json_text);
//    }
//}

void Controller::onOrderErrorReturn(CThostFtdcInputOrderField *input, CThostFtdcRspInfoField *resp) {
    if(behavior_) {
        behavior_->onOrderErrorReturn(input,resp);
    }
    if(msg_pub_){
        std::string json_text = trade::marshall( (const CThostFtdcInputOrderField*) input,resp);
        msg_pub_->publish(TopicOrderError,json_text);
    }
}

//void Controller::onOrderCancelReturn(CThostFtdcInputOrderActionField *action, CThostFtdcRspInfoField *resp){
//    if(behavior_) {
//        behavior_->onOrderCancelReturn(action,resp);
//    }
//    if(msg_pub_){
//        std::string json_text = trade::marshall(action,resp);
//        msg_pub_->publish(TopicOrderCancel,json_text);
//    }
//}

void Controller::onOrderCancelErrorReturn(CThostFtdcOrderActionField *action, CThostFtdcRspInfoField *resp) {
    if(behavior_) {
        behavior_->onOrderCancelErrorReturn(action,resp);
    }
    if(msg_pub_){
        std::string json_text = trade::marshall( (const CThostFtdcOrderActionField*)action,resp);
        msg_pub_->publish(TopicOrderCancelError,json_text);
    }
}

//成交回报
void Controller::onTradeReturn(CThostFtdcTradeField *data) {
    if(behavior_) {
        behavior_->onTradeReturn(data);
    }
    if( msg_pub_){
        std::string json_text = trade::marshall(data);
        msg_pub_->publish(TopicTrade,json_text);
    }
}


void Controller::onUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast){

}


// Tick 行情记录
void Controller::onRtnDepthMarketData( CThostFtdcDepthMarketDataField *pDepthMarketData){
    if(behavior_){
        behavior_->onTick(pDepthMarketData);
    }
    if( msg_pub_){
        if( settings_.tick_json ) {
            std::string json_text = trade::marshall(pDepthMarketData);
            msg_pub_->publish(TopicTick, json_text);
        }
        if (settings_.tick_raw){
            // msgpack encoding
            // be continue ...

        }
        if ( settings_.tick_text){
            std::vector<char> bytes;
            std::string key ;
            try {
                bytes = trade::marshall(pDepthMarketData, TickEncoding::Text, key);
                msg_pub_->publish(TopicTickText, bytes.data(), bytes.size());
                std::string name;
                std::string value(bytes.begin(), bytes.end());
                name = pDepthMarketData->InstrumentID;
                msg_pub_->publish_redis(name, key, value);
            }catch ( const std::exception& e ){
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
    }
};

void Controller::onTradeQueryResult(const std::vector< CThostFtdcTradeField >& trades ){
    if(msg_pub_){
        std::string json_text = trade::marshall(trades);
        msg_pub_->publish(TopicTradeQueryResult,json_text);
    }
}

void Controller::onPositionQueryResult(const std::vector<  CThostFtdcInvestorPositionField > & positons){
    if(msg_pub_){
        std::string json_text = trade::marshall(positons);
        msg_pub_->publish(TopicPositionQueryResult,json_text);
    }
}

void Controller::onAccountQueryResult( CThostFtdcTradingAccountField * account){
    if(msg_pub_){
        std::string json_text = trade::marshall(account);
        msg_pub_->publish(TopicAccountFunds,json_text);
    }
}

void Controller::onOrderQueryResult(const std::vector<CThostFtdcOrderField> & orders){
    if(msg_pub_){
        std::string json_text = trade::marshall(orders);
        msg_pub_->publish(TopicOrderQueryResult,json_text);
    }
}