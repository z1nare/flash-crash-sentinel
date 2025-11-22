package com.example.demo.Services;

import com.example.demo.DTOS.NewsDTO;
import com.example.demo.DTOS.TickerDTO;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class MarketListener {

    VpinCalculationService vpinService;
    JsonMapperService jsonMapperService;
    public MarketListener(VpinCalculationService vpinService,  JsonMapperService jsonMapperService) {
        this.vpinService = vpinService;
        this.jsonMapperService = jsonMapperService;
    }

    // This tells Spring: "Watch the 'market.ticks' queue.
    // When a message arrives, run this method."
    @RabbitListener(queues = "market.ticks")
    public void receiveTick(String message) {
        try{
            TickerDTO tick = jsonMapperService.mapAndValidate(message, TickerDTO.class);
            vpinService.processTick(tick);
        } catch (IllegalArgumentException e) {
            // Log validation errors but don't re-queue (malformed data)
            System.err.println("Skipping malformed Ticker message: " + e.getMessage());
        } catch (Exception e) {
            // Log other errors (e.g., JSON syntax) and let Spring decide on re-queuing
            System.err.println("Error processing TICK: " + e.getMessage());
        }
    }

    @RabbitListener(queues = "news.headlines")
    public void receiveNews(String message) {
        try {
            NewsDTO news = jsonMapperService.mapAndValidate(message, NewsDTO.class) ;
            // TODO: In Phase 1, pass to a dedicated NLP service proxy
            // nlpProxyService.sendForSentiment(news);

            System.out.println("📰 JAVA RECEIVED NEWS: " + news.getTicker() + " - " + news.getHeadline().substring(0, 30) + "...");
        } catch (Exception e) {
            System.err.println("Skipping malformed News message: " + e.getMessage());
        }
    }
}
