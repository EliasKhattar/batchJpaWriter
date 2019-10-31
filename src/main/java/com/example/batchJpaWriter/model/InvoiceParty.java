package com.example.batchJpaWriter.model;

import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.Size;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Data
public class InvoiceParty extends InvoiceProcessing{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String recordId;
    private String reserved;

    @Size(max = 8)
    private String gci;
    @Size(max = 35)
    private String customerRef;
    @Size(max = 70)
    private String invoicePartyName;
    @Size(max = 35)
    private String invoicePartyTaxId;

    private String addressLine1;

    private String addressLine2;
    @Size(max = 35)
    private String city;
    @Size(max = 3)
    private String state;
    @Size(max = 35)
    private String postalCode;
    @Size(max = 3)
    private String coutnry;
    @Size(max = 35)
    private String areaCode;
    @Size(max = 20)
    private String phoneNo;
    @Size(max = 35)
    private String extension;
    @Size(max = 35)
    private String primanyContPrefix;
    @Size(max = 35)
    private String primanyContFirstName;
    @Size(max = 35)
    private String primanyContLastName;
    @Size(max = 2)
    private String role;
    @Size(max = 35)
    private String addressLine3;
    @Size(max = 35)
    private String addressLine4;
    @Size(max = 35)
    private String addressLine5;
    @Size(max = 35)
    private String addressLine6;
    @Size(max = 35)
    private String invoicePartyTaxIdType;
    @Size(max = 35)
    private String alternateId;
    @Size(max = 35)
    private String alternateIdType;
    @Size(max = 25)
    private String isfAssociateId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "invoiceParty_id")
    private InvoiceHeader invoiceParty;
}
