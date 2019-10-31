package com.example.batchJpaWriter.batchreader;

import com.example.batchJpaWriter.model.InvoiceHeader;
import com.example.batchJpaWriter.model.InvoiceParty;
import com.example.batchJpaWriter.model.InvoiceProcessing;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.ArrayList;

import static org.apache.logging.log4j.LogManager.getLogger;

public class InvoiceProcessingItemReader implements ItemReader<InvoiceProcessing>, ItemStream {

    private static final Logger LOG = getLogger(InvoiceProcessingItemReader.class);

    private InvoiceProcessing invoiceProcessing;

    private InvoiceHeader invoiceHeader;
    private boolean recordFinished;

    private FlatFileItemReader fieldSetReader;

    // @Nullable
    @Override
    public InvoiceProcessing read() throws Exception {
        recordFinished = false;

        while (!recordFinished) {
            // System.out.println( "Item reader : " + fieldSetReader.read());
            process((InvoiceProcessing) fieldSetReader.read());
        }

        LOG.info("Mapped : " + invoiceProcessing);

        InvoiceHeader result = invoiceHeader;
        invoiceHeader = null;

        return result;
    }

    private void process(InvoiceProcessing object) throws Exception {

// finish processing if we hit the end of file


        //System.out.println("Read : " + object.toString());

        if (object == null) {
            LOG.debug("FINISHED");
            recordFinished = true;
            invoiceProcessing = null;
            return;
        }

        if (object instanceof InvoiceHeader) {
            System.out.println("Invoice header : " + object.toString());
            invoiceHeader = (InvoiceHeader) object;
        } else if (object instanceof InvoiceParty) {
            System.out.println("Invoice Party : " + object.toString());
            if (invoiceHeader.getInvoiceParty() == null) {
                invoiceHeader.setInvoiceParty(new ArrayList<InvoiceParty>());
            }
            invoiceHeader.getInvoiceParty().add((InvoiceParty) object);
            System.out.println("Invoice party array size : " + invoiceHeader.getInvoiceParty().size());
        }
    }

    public void setFieldSetReader(FlatFileItemReader<FieldSet> fieldSetReader) {
        this.fieldSetReader = fieldSetReader;
    }

    public void setInvoiceProcessing(InvoiceProcessing invoiceProcessing) {
        this.invoiceProcessing = invoiceProcessing;
    }

    @Override
    public void close() throws ItemStreamException {
        this.fieldSetReader.close();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.update(executionContext);
    }
}
