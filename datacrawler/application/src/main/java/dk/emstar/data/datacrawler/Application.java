package dk.emstar.data.datacrawler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import dk.emstar.data.datacrawler.configuration.SpringConfig;

@SpringBootApplication
@Import(SpringConfig.class)
public class Application implements CommandLineRunner {

	@Autowired
	Environment env;
	
	@Override
	public void run(String... arg0) throws Exception {
		System.out.println("hello");
	}

	public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
    }


}
