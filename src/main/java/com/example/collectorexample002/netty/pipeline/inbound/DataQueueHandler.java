package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.db.service.DataQueueService;
import com.example.collectorexample002.request.record.DataLogRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ChannelHandler.Sharable
public class DataQueueHandler extends SimpleChannelInboundHandler<DataLogRequest> {

    private final DataQueueService queueService;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DataLogRequest msg) throws Exception {

//        queueService.pushToKafka(msg);
        queueService.pushToRedis(msg);
    }
}
