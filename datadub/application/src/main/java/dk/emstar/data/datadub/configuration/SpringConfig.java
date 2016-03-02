package dk.emstar.data.datadub.configuration;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dk.emstar.data.datadub.Application;
import dk.emstar.data.datadub.repository.MetadataRepository;
import dk.emstar.data.datadub.repository.MetadataRepositoryImpl;
import dk.emstar.data.datadub.repository.TableDataRepository;
import dk.emstar.data.datadub.repository.TableDataRepositoryImpl;

@Configuration
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
@ComponentScan(basePackageClasses = {Application.class},
	excludeFilters={ @ComponentScan.Filter(type=FilterType.REGEX, pattern = {"dk.emstar.data.datadub.configuration.*"})} )
public class SpringConfig {

	@Bean
	@ConfigurationProperties(prefix="source.datasource")
	public DataSource sourceDataSource() {
		DriverManagerDataSource result = new DriverManagerDataSource();
		return result;
//	    return DataSourceBuilder
//	            .create()
//	            .build();
	}
	
	@Bean
	@ConfigurationProperties(prefix="destination.datasource")
	public DataSource destinationDataSource() {
		DriverManagerDataSource result = new DriverManagerDataSource();
		return result;
//	    return DataSourceBuilder
//	            .create()
//	            .build();
	}
	
	
	
	@Bean
	public NamedParameterJdbcOperations  sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
		return new NamedParameterJdbcTemplate(dataSource);
	}
	
	
	@Bean
	public NamedParameterJdbcOperations  destinationJdbcTemplate(@Qualifier("destinationDataSource") DataSource dataSource) {
		return new NamedParameterJdbcTemplate(dataSource);
	}

	@Bean 
	public MetadataRepository sourceMetadataRepository(@Qualifier("sourceDataSource") DataSource dataSource) {
		return new MetadataRepositoryImpl(dataSource);
	}
	
	@Bean 
	public MetadataRepository destinationMetadataRepository(@Qualifier("destinationDataSource") DataSource dataSource) {
		return new MetadataRepositoryImpl(dataSource);
	}
	

	@Bean 
	public TableDataRepository sourceTableDataRepository(@Value("${source.schema}") String schema, @Qualifier("sourceJdbcTemplate") NamedParameterJdbcOperations jdbcOperations, 
			@Qualifier("sourceMetadataRepository") MetadataRepository metadataRepository) {
		return new TableDataRepositoryImpl(schema, jdbcOperations, metadataRepository);
	}
	
	@Bean 
	public TableDataRepository destinationTableDataRepository(@Value("${destination.schema}") String schema, @Qualifier("destinationJdbcTemplate") NamedParameterJdbcOperations jdbcOperations,
			@Qualifier("destinationMetadataRepository") MetadataRepository metadataRepository) {
		return new TableDataRepositoryImpl(schema, jdbcOperations, metadataRepository);
	}

}
