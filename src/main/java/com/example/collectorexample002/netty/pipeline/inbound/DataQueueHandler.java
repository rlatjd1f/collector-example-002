package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.db.service.QueueManagerService;
import com.example.collectorexample002.request.record.CheckpointQueueData;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ChannelHandler.Sharable
public class DataQueueHandler extends SimpleChannelInboundHandler<CheckpointQueueData> {

    private final QueueManagerService queueService;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CheckpointQueueData msg) throws Exception {
        queueService.pushToKafka(msg);
        queueService.pushToRedis(msg);
    }
}
