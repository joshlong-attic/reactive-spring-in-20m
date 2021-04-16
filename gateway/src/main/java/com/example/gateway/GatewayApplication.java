package com.example.gateway;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    @Bean
    RSocketRequester rSocketRequester(RSocketRequester.Builder builder) {
        return builder.tcp("localhost", 8181);
    }

    @Bean
    WebClient http(WebClient.Builder builder) {
        return builder.build();
    }

}

@RestController
@RequiredArgsConstructor
class CustomerOrdersRestController {

    private final CrmClient crmClient;

    @GetMapping("/cos")
    Flux<CustomerOrder> get() {
        return this.crmClient.getCustomerOrders();
    }
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class Order {

    private Integer id;
    private Integer customerId;

}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Customer {
    private Integer id;
    private String name;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class CustomerOrder {
    private Customer customer;
    private List<Order> orders;
}

@Component
@RequiredArgsConstructor
class CrmClient {


    private final RSocketRequester rSocket;
    private final WebClient http;

    Flux<CustomerOrder> getCustomerOrders() {
        return getCustomers()
                .flatMap(c ->
                        Mono.zip(Mono.just(c), getOrdersFor(c.getId()).collectList())
                )
                .map(tuple -> new CustomerOrder(tuple.getT1(), tuple.getT2()));
    }

    Flux<Customer> getCustomers() {
        return http.get().uri("http://localhost:8080/customers")
                .retrieve()
                .bodyToFlux(Customer.class)
                .retryWhen(Retry.backoff(10, Duration.ofMinutes(1)))
                .onErrorResume(ex -> Flux.empty());
    }

    Flux<Order> getOrdersFor(Integer customerId) {
        return rSocket.route("orders.{customerId}", customerId)
                .retrieveFlux(Order.class);
    }
}
