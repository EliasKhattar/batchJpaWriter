package com.example.batchJpaWriter.model;



import lombok.*;

import org.springframework.format.annotation.DateTimeFormat;

import javax.persistence.*;
import javax.validation.constraints.Digits;
import javax.validation.constraints.Size;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@Entity
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Data
public class InvoiceHeader extends InvoiceProcessing{

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    /*  private final String recordId = "A";
      private final String action = "M";*/
    @Size(max = 1)
    private String recordId;
    @Size(max = 1)
    private String action;
    @Size(max = 20)
    private String invPoNo;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String invDate;
    @Size(max = 8)
    private String sellerGci;
    @Size(max = 35)
    private String sellerCustRef;
    @Size(max = 8)
    private String buyerGci;
    @Size(max = 4)
    private String delivTermCode;
    @Size(max = 35)
    private String delivTermLoc;
    @Size(max = 3)
    private String invCurr;
    @Size(max = 3)
    private String expCountry;
    @Size(max = 3)
    private String destCountry;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String exportDate;

    private String buyerSellerR;
    @Size(max = 4)
    private String invType;

    private String invTotal;
    @Size(max = 3)
    private String payTermType;
    @Size(max = 35)
    private String payTerm;

    private Double contractExchRate;
    @Size(max = 250)
    private String destContStat;
    @Size(max = 250)
    private String computedValStat;
    @Size(max = 10)
    private String totalPcs;
    @Size(max = 250)
    private String descOfGoods;
    @Size(max = 1)
    private String dangGoods;
    @Size(max = 1)
    private String na1;
    @Size(max = 1)
    private String na2;
    @Size(max = 35)
    private String na3;
    @Size(max = 35)
    private String crRef;
    @Size(max = 35)
    private String csRef;
    @Size(max = 35)
    private String c4Ref;
    @Size(max = 35)
    private String dpRef;
    @Size(max = 35)
    private String dcRef;
    @Size(max = 35)
    private String dvRef;
    @Size(max = 35)
    private String foRef;
    @Size(max = 35)
    private String inRef;
    @Size(max = 35)
    private String pdRef;
    @Size(max = 35)
    private String pjRef;
    @Size(max = 35)
    private String snRef;
    @Size(max = 35)
    private String svRef;
    @Size(max = 35)
    private String dtRef;
    @Size(max = 35)
    private String znRef;
    @Size(max = 35)
    private String authSender;
    @Size(max = 35)
    private String authSenderTitle;

    @Digits(integer = 18, fraction = 3)
    private BigDecimal dHeight;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal dWidth;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal dLenght;
    @Size(max = 3)
    private String dUom;
    private Integer pkgQty;
    @Size(max = 4)
    private String pkdType;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal grossWeight;
    @Size(max = 4)
    private String grossWeightUom;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal netWeight;
    @Size(max = 4)
    private String netWeightUom;
    @Size(max = 4)
    private String remittanceCurr;
    private String comment;
    @Size(max = 35)
    private String buyerCustRef;
    @Size(max = 35)
    private String pkgId;
    @Size(max = 35)
    private String pkgIdStart;
    @Size(max = 35)
    private String pkgIdEnd;
    @Size(max = 35)
    private String comment1;
    @Size(max = 35)
    private String comment2;
    @Size(max = 35)
    private String comment3;
    @Size(max = 35)
    private String comment4;
    @Size(max = 1)
    private String isPo;

    @Size(max = 1)
    private String firstSaleId;

    private String reserved;
    @Size(max = 40)
    private String vendorCode;
    @Size(max = 2)
    private String vendorOrderPoint;

    @OneToMany(mappedBy = "invoiceHeader", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<InvoiceParty> invoiceParty = new ArrayList<>();

    @OneToMany(mappedBy = "genericFields", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<GenericFields> genericFields = new ArrayList<>();

    public void setPkgQty(Integer pkgQty) {
        if(pkgQty == null)
            this.pkgQty = 0;
        else
            this.pkgQty = pkgQty;
    }

    public void setExportDate(String exportDate) {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

        if (exportDate.isEmpty())
            this.exportDate = String.valueOf(formatter.format(date));
        else
            this.exportDate = exportDate;
    }

    public void setContractExchRate(Double contractExchRate) {
        if(contractExchRate == null)
            this.contractExchRate = 0.0;
        else
            this.contractExchRate = contractExchRate;
    }

    public void setdHeight(BigDecimal dHeight) {
        if (dHeight == null)
            this.dHeight = BigDecimal.valueOf(0.0);
        else
            this.dHeight = dHeight;
    }

    public void setdWidth(BigDecimal dWidth) {
        if (dWidth == null)
            this.dWidth = BigDecimal.valueOf(0.0);
        else
            this.dWidth = dWidth;
    }

    public void setdLenght(BigDecimal dLenght) {
        if (dLenght == null)
            this.dLenght = BigDecimal.valueOf(0.0);
        else
            this.dWidth = dLenght;
    }

    public void setGrossWeight(BigDecimal grossWeight) {
        if (grossWeight == null)
            this.grossWeight = BigDecimal.valueOf(0.0);
        else
            this.grossWeight = grossWeight;
    }

    public void setNetWeight(BigDecimal netWeight) {
        if (netWeight == null)
            this.netWeight = BigDecimal.valueOf(0.0);
        else
            this.netWeight = netWeight;
    }

}
