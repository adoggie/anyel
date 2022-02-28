
#ifndef INNERPROC_INNERCONTROLLER_H
#define INNERPROC_INNERCONTROLLER_H

#include <memory>
#include <numeric>
#include <jsoncpp/json/json.h>
#include <boost/asio.hpp>
#include <ThostFtdcUserApiStruct.h>
#include "base.h"
#include "message.h"
#include "version.h"
#include "app.h"
#include "PosReceiver.h"
#include "behavior.h"
#include "trade.h"
#include "market.h"
#include "pubservice.h"


class Controller :  public PosListener,public  std::enable_shared_from_this<Controller> {

public:
    struct Settings {
        std::time_t start_time;          // 启动时间
        std::time_t login_time;
        std::string login_server_url;              // 登录服务器url
        std::string comm_server_ip;         // 通信服务器
        std::uint32_t comm_server_port;          // 通信服务器端口
        std::time_t establish_time;         //通信建立时间
        std::atomic_bool login_inited;           // 是否已经登录
        std::string token;                  // 接入服务的身份令牌 登录成功时返回
        int        tick_json;
        int         tick_raw;
        int         tick_text;
        Settings() {
            start_time = 0;
        }
    };
    typedef std::shared_ptr<Controller> Ptr;
public:
    Controller() {}
    bool        init(const Config &props);
    bool        open();
    void        close();
    void        run();
    void        onPositionChanged(int pos){}

    static std::shared_ptr<Controller> &instance() {
        static std::shared_ptr<Controller> handle;
        if (!handle.get()) {
            handle = std::make_shared<Controller>();
        }
        return handle;
    }

    //返回CTP交易接口
    TdApi* getTradeApi() const { return  &TdApi::instance();}
    MdApi* getMarketApi() const { return &MdApi::instance();}

    void setBehavior(Behavior* behavior){ behavior_ = behavior;}
    Json::Value getStatusInfo();

    boost::asio::io_service &io_service() {
        return io_service_;
    }
    //报单录入请求响应
//    void onOrderRespInsert(CThostFtdcInputOrderField *pInputOrder, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);

    //报单通知(内存在报单错误状态
    void onOrderReturn(CThostFtdcOrderField *order); // 委托回报
    ////委托单失败
    ///报单录入错误回报
    void onOrderErrorReturn(CThostFtdcInputOrderField * input , CThostFtdcRspInfoField *resp);

    //撤单回报 OnErrRtnOrderAction
//    void onOrderCancelReturn(CThostFtdcInputOrderActionField *action, CThostFtdcRspInfoField *resp);
    //撤单错误回报
    void onOrderCancelErrorReturn(CThostFtdcOrderActionField* action,CThostFtdcRspInfoField* resp);
    //成交回报
    void onTradeReturn(CThostFtdcTradeField *pTrade);
    //深度行情回报
    void onRtnDepthMarketData( CThostFtdcDepthMarketDataField *pDepthMarketData);
    //登录回报
    void onUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);

    void onTradeQueryResult(const std::vector< CThostFtdcTradeField >& trades );
    void onPositionQueryResult(const std::vector<  CThostFtdcInvestorPositionField > & positons);
    void onAccountQueryResult( CThostFtdcTradingAccountField * account);
    void onOrderQueryResult(const std::vector<CThostFtdcOrderField> & orders);
private:
    void workTimedTask();
    void resetStatus();
protected:

protected:
    Config                      cfgs_;
    std::recursive_mutex        rmutex_;
    boost::asio::io_service     io_service_;
    Settings                    settings_;
    std::shared_ptr<boost::asio::steady_timer>      timer_;
    int                         check_timer_interval_;
    bool                        data_inited_;
    std::time_t                 last_check_time_ = 0;
    std::time_t                 last_heart_time_ = 0;
    std::time_t                 boot_time_ ;
    Behavior*                   behavior_ = NULL;
    PosReceiver*                pos_receiver_ = NULL;
    MessagePublisher*           msg_pub_ = NULL;
};


#endif
