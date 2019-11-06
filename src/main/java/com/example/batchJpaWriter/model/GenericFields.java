package com.example.batchJpaWriter.model;

import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import javax.persistence.*;
import javax.validation.constraints.Digits;
import javax.validation.constraints.Size;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Data
public class GenericFields extends InvoiceProcessing{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String recordId;
    private String reserved;

    @Size(max = 10)
    private String code;
    @Size(max = 500)
    private String ref;
    @DateTimeFormat(pattern = "MM/dd/yyyy")
    private String date;
    @Digits(integer = 18, fraction = 3)
    private BigDecimal qty;
    @Size(max = 4)
    private String units;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "genericFields_id")
    private InvoiceHeader genericFields;

    public void setDate(String date) {
        Date now = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

        if (date.isEmpty())
            this.date = String.valueOf(formatter.format(now));
        else
            this.date = date;
        this.date = date;
    }

    public void setQty(BigDecimal qty) {
        if (qty == null)
            this.qty = BigDecimal.valueOf(0.0);
        else
            this.qty = qty;
    }

    public void setUnits(String units) {
        if(units.isEmpty())
            this.units = "NA";
        else
            this.units = units;
    }
}
