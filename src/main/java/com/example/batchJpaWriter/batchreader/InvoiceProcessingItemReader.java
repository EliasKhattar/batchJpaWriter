package com.example.batchJpaWriter.batchreader;

import com.example.batchJpaWriter.model.*;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;

public class InvoiceProcessingItemReader implements ItemReader<InvoiceProcessing>, ItemStream {

    private static final Logger LOG = getLogger(InvoiceProcessingItemReader.class);

    private InvoiceProcessing invoiceProcessing;

    private InvoiceHeader invoiceHeader;
    private Declaration declaration;
    private boolean recordFinished;

    private FlatFileItemReader fieldSetReader;

    @Override
    public InvoiceProcessing read() throws Exception {
        recordFinished = false;

        while (!recordFinished) {
            process((InvoiceProcessing) fieldSetReader.read());
        }

        InvoiceHeader result = invoiceHeader;
        invoiceHeader = null;

        return result;
    }

    private void process(InvoiceProcessing object) throws Exception {

        // finish processing if we hit the end of file
        if (object == null) {
            LOG.debug("FINISHED");
            recordFinished = true;
            //invoiceProcessing = null;
            return;
        }

         if (object instanceof InvoiceHeader) {
            //System.out.println("Invoice header : " + object.toString());
            invoiceHeader = (InvoiceHeader) object;
        } else if (object instanceof InvoiceParty) {
            System.out.println("Invoice Party : " + object.toString());
            if (invoiceHeader.getInvoiceParty() == null) {
                invoiceHeader.setInvoiceParty(new ArrayList<>());
            }

            //invoiceHeader.getInvoiceParty().add((InvoiceParty) object);
            InvoiceParty invoiceParty = (InvoiceParty) object;
            invoiceParty.setInvoiceHeader(invoiceHeader);
            invoiceHeader.getInvoiceParty().add(invoiceParty);

           // System.out.println("Invoice header after adding party : " + invoiceHeader.getInvoiceParty().toString());
        }else if (object instanceof GenericFields){
            if(invoiceHeader.getGenericFields() == null){
                invoiceHeader.setGenericFields(new ArrayList<>());
            }
            GenericFields genericFields = (GenericFields) object;
            genericFields.setGenericFields(invoiceHeader);
            invoiceHeader.getGenericFields().add(genericFields);
        }else if(object instanceof Declaration){

            declaration = (Declaration) object;
             System.out.println("Declaration : " + declaration.toString());
        }
    }

    public void setFieldSetReader(FlatFileItemReader<FieldSet> fieldSetReader) {
        this.fieldSetReader = fieldSetReader;
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