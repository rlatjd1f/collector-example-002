package com.example.collectorexample002.netty.pipeline.inbound;

import com.example.collectorexample002.db.service.QueueManageService;
import com.example.collectorexample002.netty.request.record.CheckpointQueueData;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
@ChannelHandler.Sharable
public class DataQueueHandler extends SimpleChannelInboundHandler<CheckpointQueueData> {

    private final QueueManageService queueService;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CheckpointQueueData msg) throws Exception {
        try {
            queueService.pushToKafka(msg);
        } catch (Exception e) {
            ctx.fireExceptionCaught(new RuntimeException("[DATA_QUEUE] Kafka 큐 삽입 실패"));
        }

        try {
            queueService.pushToRedis(msg);
        } catch (Exception e) {
            ctx.fireExceptionCaught(new RuntimeException("[DATA_QUEUE] Redis 큐 삽입 실패"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    }
}
