//
// Created by scott on 2021/11/2.
//

#include <thread>
#include <zmq.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "PosReceiver.h"

bool    ZMQ_PosReceiver::init(const Config& cfg) {
    return true;
}


bool    ZMQ_PosReceiver::open(){
    ctx_ = zmq_ctx_new ();
    void *subscriber = zmq_socket (ctx_, ZMQ_SUB); // ������Ϣģʽ
    if (subscriber == NULL){
        return false;
    }
    int ret = zmq_connect (subscriber, cfg_.get_string("zmq.pos.pub_addr","tcp://127.0.0.1:9901").c_str());
    if (ret < 0){
        zmq_ctx_destroy(subscriber);
        return false;
    }
    std::string topic = cfg_.get_string("ctp.account");
    zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, topic.c_str(), topic.size());

    int iRcvTimeout = cfg_.get_int("zmq.pos.recv_timeout", 60000); // millsecond
    if(zmq_setsockopt(subscriber, ZMQ_RCVTIMEO, &iRcvTimeout, sizeof(iRcvTimeout)) < 0){
        zmq_close(subscriber);
        zmq_ctx_destroy(subscriber);
        return false;
    }
    subscriber_ = subscriber;
    thread_ = std::thread( &ZMQ_PosReceiver::recv_thread , this);
    return true ;
}

void    ZMQ_PosReceiver::close(){

}

void ZMQ_PosReceiver::recv_thread(){
    running_ = true;
    while(running_){
        char szBuf[1024] = {0};
        int ret = zmq_recv(subscriber_, szBuf, sizeof(szBuf) - 1, 0);
        if (ret > 0){
            std::vector<std::string> ss;
            boost::split(ss,szBuf,boost::is_any_of("_"));
            if( ss.size() > 1 ){
                std::string text = ss[1];
                int pos = boost::lexical_cast<int>(text);
                if( listener_){
                    listener_->onPositionChanged(pos);
                }
            }

        }else{
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}
