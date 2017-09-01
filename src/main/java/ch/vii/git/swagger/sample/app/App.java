package ch.vii.git.swagger.sample.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = { "ch.vii.git.swagger.sample.app", "ch.vii.git.swagger.sample.rest" })
public class App {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}