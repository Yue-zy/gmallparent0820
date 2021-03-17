package com.atguigu.gmall.activity.receiver;

import com.atguigu.gmall.activity.mapper.SeckillGoodsMapper;
import com.atguigu.gmall.activity.service.SeckillGoodsService;
import com.atguigu.gmall.activity.util.CacheHelper;
import com.atguigu.gmall.common.constant.MqConst;
import com.atguigu.gmall.common.constant.RedisConst;
import com.atguigu.gmall.common.util.DateUtil;
import com.atguigu.gmall.model.activity.SeckillGoods;
import com.atguigu.gmall.model.activity.UserRecode;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.rabbitmq.client.Channel;
import lombok.SneakyThrows;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

/**
 * @author mqx
 * @date 2021-3-8 08:55:38
 */
//  监听秒杀商品的消息队列
@Component
public class SeckillReceiver {

    @Autowired
    private SeckillGoodsMapper seckillGoodsMapper;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private SeckillGoodsService seckillGoodsService;


    //   rabbitService.sendMessage(MqConst.EXCHANGE_DIRECT_TASK,MqConst.ROUTING_TASK_1,"来吧!");
    @SneakyThrows
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = MqConst.QUEUE_TASK_1,durable = "true",autoDelete = "false"),
            exchange = @Exchange(value = MqConst.EXCHANGE_DIRECT_TASK),
            key = {MqConst.ROUTING_TASK_1}
    ))
    public void importToRedis(String msg , Message message, Channel channel){
        //  查询数据库中当前秒杀的商品放入缓存！ new Date(), status=1,stock_count>0
        QueryWrapper<SeckillGoods> seckillGoodsQueryWrapper = new QueryWrapper<>();
        seckillGoodsQueryWrapper.eq("status","1").gt("stock_count",0);
        //  当前系统时间，只判断年月日 将时间做个处理并比较！
        seckillGoodsQueryWrapper.eq("date_format(start_time,'%Y-%m-%d')", DateUtil.formatDate(new Date()));
        //  当前秒杀商品集合
        List<SeckillGoods> seckillGoodsList = seckillGoodsMapper.selectList(seckillGoodsQueryWrapper);
        //  将商品放入缓存
        if (!CollectionUtils.isEmpty(seckillGoodsList)){
            //  循环遍历
            for (SeckillGoods seckillGoods : seckillGoodsList) {
                //  缓存应该采用什么数据类型存储：采用hash 格式存储！
                //  hset(key,field,value) hget(key,value); field=seckillGoods.getSkuId();
                //  组成key=seckill:goods field=seckillGoods.getSkuId(); value = seckillGoods;
                String seckillKey = RedisConst.SECKILL_GOODS;
                //  判断一下在缓存中是否有了当前的秒商品，
                Boolean flag = redisTemplate.boundHashOps(seckillKey).hasKey(seckillGoods.getSkuId().toString());
                //  如果flag=true;
                if (flag){
                    //  表示缓存中已经有这个商品了，则不用再重复添加，跳过当前的循环操作
                    continue;
                }
                //  如果flag=false; 表示缓存中没有数据，将商品放入缓存
                redisTemplate.boundHashOps(seckillKey).put(seckillGoods.getSkuId().toString(),seckillGoods);
                //  如何控制库存超买? 利用缓存存储数据的机制来实现！商品的数量skuId = 40 ,num = 10 ,stockCount = 10 ,10 用redis 的list 数据类型进行存储！
                for (Integer i = 0; i < seckillGoods.getStockCount(); i++) {
                    //  定义key= seckill:stock:skuId
                    String seckillNumKey = RedisConst.SECKILL_STOCK_PREFIX+seckillGoods.getSkuId().toString();
                    redisTemplate.boundListOps(seckillNumKey).leftPush(seckillGoods.getSkuId().toString());
                }
                //  初始化状态为 1 可以秒杀了！
                redisTemplate.convertAndSend("seckillpush",seckillGoods.getSkuId()+":1");
                //  后续业务：如果商品没有库存了，这个状态位应该0  redisTemplate.convertAndSend("seckillpush",seckillGoods.getSkuId()+":0");
                //  String stat = CacheHelper.get("skuId"); stat.equals(0) 那么你是没有办法秒杀商品的！

            }
            //  手动确认消息
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        }
    }
    //  监听队列中的用户UserRecode;
    @SneakyThrows
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = MqConst.QUEUE_SECKILL_USER,durable = "true",autoDelete = "false"),
            exchange = @Exchange(value = MqConst.EXCHANGE_DIRECT_SECKILL_USER),
            key = {MqConst.ROUTING_SECKILL_USER}
    ))
    public void seckillUserRecode(UserRecode userRecode,Message message,Channel channel){
        if (userRecode!=null){
            //  编写要给方法即可！
            seckillGoodsService.seckillOrder(userRecode.getSkuId(),userRecode.getUserId());
            //  手动确认：
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        }

    }

}
