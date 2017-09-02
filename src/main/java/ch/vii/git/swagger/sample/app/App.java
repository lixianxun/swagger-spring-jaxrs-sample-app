package ch.vii.git.swagger.sample.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/*@Configuration
@Import({
	DispatcherServletAutoConfiguration.class,
	EmbeddedServletContainerAutoConfiguration.class,
	ErrorMvcAutoConfiguration.class,
	HttpEncodingAutoConfiguration.class,
	HttpMessageConvertersAutoConfiguration.class,
	JacksonAutoConfiguration.class,
	JerseyAutoConfiguration.class,
	JmxAutoConfiguration.class,
	MultipartAutoConfiguration.class,
	PropertyPlaceholderAutoConfiguration.class,
	ServerPropertiesAutoConfiguration.class,
	ValidationAutoConfiguration.class,
	WebClientAutoConfiguration.class,
	WebSocketAutoConfiguration.class
})*/
@SpringBootApplication
@ComponentScan(basePackages = { "ch.vii.git.swagger.sample.app", "ch.vii.git.swagger.sample.rest" })
public class App {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}