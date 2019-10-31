package com.example.batchJpaWriter.configuration;

import com.example.batchJpaWriter.batchreader.InvoiceProcessingItemReader;
import com.example.batchJpaWriter.model.InvoiceHeader;
import com.example.batchJpaWriter.model.InvoiceParty;
import com.example.batchJpaWriter.model.InvoiceProcessing;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private EntityManagerFactory emf;

    public final String  START_LINE =  "AZ";

    @Bean
    public Step ipStep() throws Exception{
        return stepBuilderFactory.get("ipStep")
                .<InvoiceProcessing,InvoiceProcessing>chunk(5)
                //.reader(reader(null,null))
                .reader(reader())
                .writer(jpaItemWriter())
                .build();
    }

    @Bean
    public Job ipUserJob() throws Exception {
        return jobBuilderFactory.get("ipJob")
                .incrementer(new RunIdIncrementer())
                //.start(step1())// pointing to the correct line where the reader should start reading in ipStep.
                //.next(ipStep())
                .flow(ipStep())
                .end()
                .build();
    }

    @Bean
    public Step step1() throws Exception {

        return stepBuilderFactory.get("calculateLinesToSkip")
                .tasklet((contribution, chunkContext) -> {

                    // int linesToSkip = 22;//count(null); do the math here
                    Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();
                    String filePath = (String) jobParameters.get("file_path");
                    int linesToSkip = count(filePath);
                    chunkContext.getStepContext().getStepExecution().getJobExecution()
                            .getExecutionContext().put("linesToSkip", linesToSkip);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * Creating a count method to count the lines that should be skipped before start reading and mapping the data
     *@return Integer
     */

    @StepScope
    public int count(@Value("#{jobParameters[file_path]}") String filePath) throws Exception {

        final FileSystemResource fileSystemResource = new FileSystemResource(filePath);
        FlatFileItemReader<String> flatFileItemReader = new FlatFileItemReader();
        flatFileItemReader.setResource(fileSystemResource);

        File source = new File(filePath);

        Scanner scanner = new Scanner(source);
        int count = 0;

        while(scanner.hasNext()) {
            String x = scanner.next();
            String arr = x;
            count++;
            if (START_LINE.equals(arr)) {
                System.out.println("Lines to skip " + count);
                scanner.close();
                return count;
            }/*else{
               System.out.println("File does not have header AZ!!!");
               System.out.println("arr : " + arr);
           }*/

        }
        scanner.close();
        return count;
    }

    @Bean
    public FlatFileItemReader readers(Integer linesToSkip) throws Exception {

        final FileSystemResource fileSystemResource = new FileSystemResource("BOM9007134_1568194686037.ip");

        return new FlatFileItemReaderBuilder()
                .name("testItemReader")
                .resource(fileSystemResource)
                .linesToSkip(linesToSkip)
                .lineMapper(ipLineMapper())
                .build();
    }

    @Bean
    public InvoiceProcessingItemReader reader() throws Exception {

        InvoiceProcessingItemReader invoiceProcessingItemReader = new InvoiceProcessingItemReader();
        invoiceProcessingItemReader.setFieldSetReader(readers(21));

        return invoiceProcessingItemReader;
    }

    @Bean
    public JpaItemWriter jpaItemWriter(){
        JpaItemWriter writer = new JpaItemWriter();
        writer.setEntityManagerFactory(emf);
        return writer;
    }

    @Bean
    public PatternMatchingCompositeLineMapper ipLineMapper() throws Exception {

        PatternMatchingCompositeLineMapper mapper = new PatternMatchingCompositeLineMapper();

        Map<String, LineTokenizer> tokenizers = new HashMap<String, LineTokenizer>();
        tokenizers.put("A*",invoiceHeaderTokenizer() );
        tokenizers.put("I*",invoicePartyTokenizer());


        mapper.setTokenizers(tokenizers);

        Map<String, FieldSetMapper> mappers = new HashMap<String, FieldSetMapper>();
        mappers.put("A*", invoiceHeaderFieldSetMapper());
        mappers.put("I*", invoicePartyFieldSetMapper());


        mapper.setFieldSetMappers(mappers);

        return mapper;

    }

    /**
     * Creating Tokenizer and Mapper for Invoice Header
     *@return InvoiceHeader object
     */

    @Bean
    public LineTokenizer invoiceHeaderTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setQuoteCharacter(DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER);
        tokenizer.setNames(new String[] { "recordId", "action", "invPoNo", "invDate", "sellerGci", "sellerCustRef", "buyerGci", "delivTermCode", "delivTermLoc", "invCurr", "expCountry", "destCountry","exportDate","buyerSellerR",
                "invType","invTotal","payTermType","payTerm","contractExchRate","destContStat","computedValStat","totalPcs","descOfGoods","dangGoods","na1","na2","na3","crRef","csRef","c4Ref","dpRef","dcRef","dvRef","foRef",
                "inRef","pdRef","pjRef","snRef","svRef","dtRef","znRef","authSender","authSenderTitle","dHeight","dWidth","dLenght","dUom","pkgQty","pkdType","grossWeight","grossWeightUom","netWeight","netWeightUom",
                "remittanceCurr","comment","buyerCustRef","pkgId","pkgIdStart","pkgIdEnd","comment1","comment2","comment3","comment4","isPo","firstSaleId","reserved","vendorCode","vendorOrderPoint"});
        return tokenizer;
    }

    @Bean
    public FieldSetMapper<InvoiceHeader> invoiceHeaderFieldSetMapper() throws Exception {
        BeanWrapperFieldSetMapper<InvoiceHeader> mapper =
                new BeanWrapperFieldSetMapper<InvoiceHeader>();

        mapper.setPrototypeBeanName("invoiceHeader");
        mapper.afterPropertiesSet();
        return mapper;
    }

    @Bean
    @Scope("prototype")
    public InvoiceHeader invoiceHeader() {
        return new InvoiceHeader();
    }

    /**
     * Creating Tokenizer and Mapper for Invoice Party
     *@return InvoiceParty object
     */

    @Bean
    public LineTokenizer invoicePartyTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setQuoteCharacter(DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER);
        tokenizer.setNames(new String[] { "recordId", "reserved","gci","customerRef","invoicePartyName","invoicePartyTaxId","addressLine1","addressLine2","city","state","postalCode","country","areaCode","phoneNo","extension"
                ,"primanyContPrefix","primanyContFirstName","primanyContLastName","role","addressLine3","addressLine4","addressLine5","addressLine6","invoicePartyTaxIdType","alternateId"
                ,"alternateIdType","isfAssociateId"});
        return tokenizer;
    }

    @Bean
    public FieldSetMapper<InvoiceParty> invoicePartyFieldSetMapper() throws Exception {
        BeanWrapperFieldSetMapper<InvoiceParty> mapper =
                new BeanWrapperFieldSetMapper<InvoiceParty>();

        mapper.setPrototypeBeanName("invoiceParty");
        mapper.afterPropertiesSet();
        return mapper;
    }

    @Bean
    @Scope("prototype")
    public InvoiceParty invoiceParty() {
        return new InvoiceParty();
    }

}
