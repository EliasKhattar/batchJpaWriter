package com.example.batchJpaWriter.model;

import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.Digits;
import javax.validation.constraints.Size;
import java.math.BigDecimal;

@Entity
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Declaration extends InvoiceProcessing {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String recordId;
    private String action;

    @Size(max = 35)
    private String declNo;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String declDate;
    @Size(max = 35)
    private String refNo;
    @Size(max = 35)
    private String etmsFileNo;
    @Size(max = 35)
    private String hbNo;
    @Size(max = 35)
    private String mbNo;
    @Size(max = 35)
    private String cargoControlNo;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal internationalTransChargs;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal insurance;
    @Size(max = 4)
    private String modeOfTransp;
    @Size(max = 3)
    private String originCode;
    @Size(max = 3)
    private String destCode;
    @Size(max = 35)
    private String voyFlightNo;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String voyFlightDate;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal airFrghtChrgDisc;
    @Size(max = 5)
    private String portOfLand;
    @Size(max = 5)
    private String portOfImpCode;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String arrivalPortImportDate;
    @Size(max = 5)
    private String portOfDeclCode;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String arrivalPortDeclDate;
    @Size(max = 7)
    private String lloydsCode;
    @Size(max = 35)
    private String vesselName;
    @Size(max = 2)
    private String declType;
    @Size(max = 2)
    private String entryProcdType;
    @Size(max = 4)
    private String carrierCode;
    @Size(max = 1)
    private String containerized;
    @Size(max = 35)
    private String cargoLoc;
    @Size(max = 35)
    private String itNumber;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String itDate;
    private Integer manifestQty;
    @Size(max = 4)
    private String manifestUom;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal grossWght;
    @Size(max = 4)
    private String grossWghtUom;
    @Size(max = 3)
    private String customsPortCode;
    @Size(max = 3)
    private String portUnladingCode;
    @Size(max = 3)
    private String interTranspChargCurr;
    @Size(max = 3)
    private String insuranceCurr;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal volume;
    @Size(max = 3)
    private String volumeUom;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal netWegiht;
    @Size(max = 4)
    private String netWegihtUom;
    @Size(max = 5)
    private String customsCateg;
    @Size(max = 4)
    private String isfBillingOfLadingIssuer;
    @DateTimeFormat(pattern = "MM/dd/yyyy hh:mm:ss")
    private String isfCutOffDate;
    @Digits(integer = 17, fraction = 9)
    private BigDecimal isuranceExchangeRate;
    @Digits(integer = 17, fraction = 9)
    private BigDecimal transpExchangeRate;
    @Size(max = 30)
    private String isfVendorId;



    public void setInternationalTransChargs(BigDecimal internationalTransChargs) {
        if(internationalTransChargs == null)
            this.internationalTransChargs = BigDecimal.valueOf(0.0);
        else
            this.internationalTransChargs = internationalTransChargs;
    }

    public void setInsurance(BigDecimal insurance) {
        if(insurance == null)
            this.insurance = BigDecimal.valueOf(0.0);
        else
            this.insurance = insurance;
    }
}
