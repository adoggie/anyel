//
// Created by scott on 2021/11/2.
// 仓位接收器

#ifndef ELXTRADER_POSRECEIVER_H
#define ELXTRADER_POSRECEIVER_H

#include <atomic>

#include "config.h"


/*
 * macos : brew install zeromq
 *
 */
struct PosListener{
    virtual void onPositionChanged(int) = 0;
};

class PosReceiver {
public:
    void    setListener( PosListener* listener){ listener_ = listener;}
    virtual bool    init(const Config& cfg) { cfg_ = cfg ; return true;};
    virtual bool    open(){ return true;}
    virtual void    close(){}
protected:
    PosListener * listener_;
    Config  cfg_;

};



class ZMQ_PosReceiver:public PosReceiver{
public:

protected:
    bool    init(const Config& cfg) ;
    bool    open();
    void    close();
private:
    void recv_thread();

    std::atomic_bool running_;
    std::thread thread_;
    void*   ctx_;
    void *  subscriber_;
};


#endif //ELXTRADER_POSRECEIVER_H
