
Skip to content
Pull requests
Issues
Marketplace
Explore
@Thirapathireddy
Thirapathireddy /
Thiru
Private

0
0

    0

Code
Issues 0
Pull requests 0
Actions
Projects 0
Security 0
Insights

    Settings

Delete load_converted_rds_data.usql

    master 

@Thirapathireddy
Thirapathireddy committed on May 21, 2019
1 parent 90f96cc commit 439e74cf3e718d36fbb441f31f83829b9fbe218e
Showing
with 0 additions and 6,735 deletions.

    +0 −6,735 

    load_converted_rds_data.usql

6,735 load_converted_rds_data.usql
@@ -1,6735 +0,0 @@
﻿DECLARE EXTERNAL @rdsFolderPath string = "/FIONA/RDS/Delhaize/POS/TLog/2017/01/01/000000/converted";
DECLARE EXTERNAL @adfPipelineId string = "hist_load_20170101";

SET @@FeaturePreviews = "InputFileGrouping:on,FileSetV2Dot5:on,AsyncCompilerStoreAccess:on";

ALTER TABLE pos.storeline.rds_load_stats ADD IF NOT EXISTS PARTITION(@adfPipelineId);

// pulse_strln_tckt_strt
DECLARE @pulse_strln_tckt_strt_path string = string.Format("{0}/ticketstart.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_tckt_strt_path) AND FILE.LENGTH(@pulse_strln_tckt_strt_path) > 0 THEN
    @pulse_strln_tckt_strt = 
        EXTRACT 
            Opcode int,                 
            TicketStart short?,
            NoSale short?,
            RecalledTicket short?,
            VoidTicket short?,
            TenderPurchase short?,
            opt_wic_ticket short?,
            opt_freq_shop short?,
            return_type int?,
            recall_ticket_no int?,
            wic_issue_day int?,
            wic_issue_year int?,
            wic_exp_day int?,
            wic_exp_year int?,
            wic_issue_month int?,
            wic_exp_month int?,
            wic_amount decimal?,
            recall_pos_no int?,
            SelfScanningTicket short?,
            TailDoNotProcess short?,
            TailBadRecord short?,
            TailTrainingMode short?,
            TailPosOffline short?,
            TailVoidTicket short?,
            TailReturnTicket int?,       
            TailTV int,
            Unique int,
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string,
            TailCashierNo int,
            TailPcNo int,
            TailPosNo int,
            TailTicketNumber int,
            TailSequenceNumber int,
            TicketStart_DT DateTime?,
            TicketStart_TM string,
            Src_T_E string
        FROM @pulse_strln_tckt_strt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_tckt_strt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_tckt_strt (
        pulse_strln_store_nbr,
        pulse_strln_cshr_nbr,
        pulse_strln_rgstr_nbr,
        pulse_strln_tckt_nbr,
        pulse_strln_tckt_dt,
        pulse_strln_tckt_tm,
        pulse_strln_seq_nbr,
        pulse_strln_oprtn_cd,
        pulse_strln_tckt_strt_no_sale_ind,
        pulse_strln_tckt_strt_rcld_tckt_ind,
        pulse_strln_tckt_strt_self_scan_tckt_ind,
        pulse_strln_tckt_strt_tnd_pur_ind,
        pulse_strln_tckt_strt_ind,
        pulse_strln_tckt_strt_void_tckt_ind,
        pulse_strln_tckt_strt_freq_shop_ind,
        pulse_strln_tckt_strt_wic_tckt_ind,
        pulse_strln_tckt_strt_recall_pos_nbr,
        pulse_strln_tckt_strt_recall_tckt_nbr,
        pulse_strln_tckt_strt_rtn_typ,
        pulse_strln_tckt_strt_wic_amt,
        pulse_strln_tckt_strt_wic_xpir_day,
        pulse_strln_tckt_strt_wic_xpir_month,
        pulse_strln_tckt_strt_wic_xpir_yr,
        pulse_strln_tckt_strt_wic_issue_day,
        pulse_strln_tckt_strt_wic_issue_month,
        pulse_strln_tckt_strt_wic_issue_yr,
        pulse_strln_tckt_strt_from_evt_ind,
        pulse_strln_bad_rcd_ind,
        pulse_strln_do_not_proc_ind,
        pulse_strln_pos_offln_ind,
        pulse_strln_rtn_tckt,
        pulse_strln_tv,
        pulse_strln_train_mode_ind,
        pulse_strln_void_tckt_ind,
        last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)    
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr,
        TailCashierNo AS pulse_strln_cshr_nbr,
        TailPosNo AS pulse_strln_rgstr_nbr,
        TailTicketNumber AS pulse_strln_tckt_nbr,
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt,
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm,
        TailSequenceNumber AS pulse_strln_seq_nbr,
        Opcode AS pulse_strln_oprtn_cd,
        (short)(NoSale ?? 0) AS pulse_strln_tckt_strt_no_sale_ind,
        (short)(RecalledTicket ?? 0) AS pulse_strln_tckt_strt_rcld_tckt_ind,
        (short)(SelfScanningTicket ?? 0) AS pulse_strln_tckt_strt_self_scan_tckt_ind,
        (short)(TenderPurchase ?? 0) AS pulse_strln_tckt_strt_tnd_pur_ind,
        (short)(TicketStart ?? 0) AS pulse_strln_tckt_strt_ind,
        (short)(VoidTicket ?? 0) AS pulse_strln_tckt_strt_void_tckt_ind,
        (short)(opt_freq_shop ?? 0) AS pulse_strln_tckt_strt_freq_shop_ind,
        (short)(opt_wic_ticket ?? 0) AS pulse_strln_tckt_strt_wic_tckt_ind,
        (int)(recall_pos_no ?? 0) AS pulse_strln_tckt_strt_recall_pos_nbr,
        (int)(recall_ticket_no ?? 0) AS pulse_strln_tckt_strt_recall_tckt_nbr,
        (int)(return_type ?? 0) AS pulse_strln_tckt_strt_rtn_typ,
        (decimal)(wic_amount ?? 0) / 100 AS pulse_strln_tckt_strt_wic_amt,
        (int)(wic_exp_day ?? 0) AS pulse_strln_tckt_strt_wic_xpir_day,
        (int)(wic_exp_month ?? 0) AS pulse_strln_tckt_strt_wic_xpir_month,
        (int)(wic_exp_year ?? 0) AS pulse_strln_tckt_strt_wic_xpir_yr,
        (int)(wic_issue_day ?? 0) AS pulse_strln_tckt_strt_wic_issue_day,
        (int)(wic_issue_month ?? 0) AS pulse_strln_tckt_strt_wic_issue_month,
        (int)(wic_issue_year ?? 0) AS pulse_strln_tckt_strt_wic_issue_yr,
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_tckt_strt_from_evt_ind,
        (short)(TailBadRecord ?? 0) AS pulse_strln_bad_rcd_ind,
        (short)(TailDoNotProcess ?? 0) AS pulse_strln_do_not_proc_ind,
        (short)(TailPosOffline ?? 0) AS pulse_strln_pos_offln_ind,
        (int)(TailReturnTicket ?? 0) AS pulse_strln_rtn_tckt,
        TailTV AS pulse_strln_tv,
        (short)(TailTrainingMode ?? 0) AS pulse_strln_train_mode_ind,
        (short)(VoidTicket ?? 0) AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_tckt_strt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)  
    SELECT "pulse_strln_tckt_strt" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt        
    FROM @pulse_strln_tckt_strt
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_tckt_end 
DECLARE @pulse_strln_tckt_end_path string = string.Format("{0}/ticketend.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_tckt_end_path) AND FILE.LENGTH(@pulse_strln_tckt_end_path) > 0 THEN
    @pulse_strln_tckt_end = 
        EXTRACT 
            Opcode int,
            TicketTotal int,
            VoidTicket short,
            SaveTicket short,
            RecallTicket short,
            SendToQbuster short,
            TaxValue int,
            ItemsNo int,
            Amount decimal,
            Discount short,
            NetAmount decimal,
            nt_discount decimal,
            GrandTotal long,
            FoodStampSales int,
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int,
            TailTV int,
            Unique int,
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string,
            TailCashierNo int,
            TailPcNo int,
            TailPosNo int,
            TailTicketNumber int,
            TailSequenceNumber int,
            TicketStart_DT DateTime?,
            TicketStart_TM string,
            Src_T_E string        
        FROM @pulse_strln_tckt_end_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_tckt_end ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_tckt_end (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_tckt_end_amt, 
        pulse_strln_tckt_end_disc_ind, 
        pulse_strln_tckt_end_fd_stmp_sls, 
        pulse_strln_tckt_end_grand_tot, 
        pulse_strln_tckt_end_items_nbr, 
        pulse_strln_tckt_end_net_amt, 
        pulse_strln_tckt_end_net_disc, 
        pulse_strln_tckt_end_recall_tckt_ind, 
        pulse_strln_tckt_end_save_tckt_ind, 
        pulse_strln_tckt_end_send_to_qbuster_ind, 
        pulse_strln_tckt_end_tax_val, 
        pulse_strln_tckt_end_tckt_tot, 
        pulse_strln_tckt_end_void_tckt_ind, 
        pulse_strln_tckt_end_from_evt_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_tckt_end_uniq, 
        transaction_id
    )   
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_tckt_end_amt, 
        Discount AS pulse_strln_tckt_end_disc_ind, 
        FoodStampSales AS pulse_strln_tckt_end_fd_stmp_sls, 
        GrandTotal AS pulse_strln_tckt_end_grand_tot, 
        ItemsNo AS pulse_strln_tckt_end_items_nbr, 
        (decimal)(NetAmount / 100) AS pulse_strln_tckt_end_net_amt, 
        (decimal)(nt_discount / 100) AS pulse_strln_tckt_end_net_disc, 
        RecallTicket AS pulse_strln_tckt_end_recall_tckt_ind, 
        SaveTicket AS pulse_strln_tckt_end_save_tckt_ind, 
        SendToQbuster AS pulse_strln_tckt_end_send_to_qbuster_ind, 
        TaxValue AS pulse_strln_tckt_end_tax_val, 
        TicketTotal AS pulse_strln_tckt_end_tckt_tot, 
        VoidTicket AS pulse_strln_tckt_end_void_tckt_ind, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_tckt_end_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_tckt_end_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id         
    FROM @pulse_strln_tckt_end;  

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_tckt_end" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_tckt_end
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_dept_sale 
DECLARE @pulse_strln_dept_sale_path string = string.Format("{0}/departmentsale.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_dept_sale_path) AND FILE.LENGTH(@pulse_strln_dept_sale_path) > 0 THEN
    @pulse_strln_dept_sale = 
        EXTRACT 
            Opcode int,
            DepartmentNo int,
            Subtract short, 
            Cancel short, 
            Negative short, 
            opt_was_cancelled short, 
            opt_bottle_deposit short, 
            opt_bottle_refund short, 
            opt_enh_refund_bonus short, 
            opt_price_override short, 
            ManualPrice short, 
            WeightFromScale short, 
            QtyIsWeight short, 
            opt_non_merch short, 
            StoreCoupon short, 
            VendorCoupon short, 
            FSpayment short, 
            opt_staff_discountable short, 
            RefundDpt int, 
            ReturnType int, 
            tax_ptr int, 
            product_code int, 
            Qty int, 
            Price decimal, 
            Amount decimal, 
            NoTaxAmount decimal, 
            AtQty int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_dept_sale_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_dept_sale ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_dept_sale (
        pulse_strln_store_nbr,
        pulse_strln_cshr_nbr,
        pulse_strln_rgstr_nbr,
        pulse_strln_tckt_nbr,
        pulse_strln_tckt_dt,
        pulse_strln_tckt_tm,
        pulse_strln_seq_nbr,
        pulse_strln_oprtn_cd,
        pulse_strln_dept_sale_amt,
        pulse_strln_dept_sale_at_qty,
        pulse_strln_dept_sale_cancel_ind,
        pulse_strln_dept_sale_dept_nbr,
        pulse_strln_dept_sale_fd_stmp_pmt_ind,
        pulse_strln_dept_sale_manl_prc_ind,
        pulse_strln_dept_sale_neg_ind,
        pulse_strln_dept_sale_no_tax_amt,
        pulse_strln_dept_sale_prc,
        pulse_strln_dept_sale_prdt_cd,
        pulse_strln_dept_sale_qty,
        pulse_strln_dept_sale_qty_wgt_ind,
        pulse_strln_dept_sale_rfnd_dept,
        pulse_strln_dept_sale_rtn_typ,
        pulse_strln_dept_sale_store_cpn_ind,
        pulse_strln_dept_sale_subtrct_ind,
        pulse_strln_dept_sale_tax_pntr,
        pulse_strln_dept_sale_vend_cpn_ind,
        pulse_strln_dept_sale_wgt_from_scale_ind,
        pulse_strln_dept_sale_bottle_depst_ind,
        pulse_strln_dept_sale_bottle_rfnd_ind,
        pulse_strln_dept_sale_enhcd_rfnd_bonus_ind,
        pulse_strln_dept_sale_non_mdse_ind,
        pulse_strln_dept_sale_prc_ovrd_ind,
        pulse_strln_dept_sale_staff_disc_ind,
        pulse_strln_dept_sale_was_cancel_ind,
        pulse_strln_bad_rcd_ind,
        pulse_strln_dt,
        pulse_strln_tm,
        pulse_strln_do_not_proc_ind,
        pulse_strln_term_nbr,
        pulse_strln_pos_offln_ind,
        pulse_strln_rtn_tckt,
        pulse_strln_tv,
        pulse_strln_train_mode_ind,
        pulse_strln_void_tckt_ind,
        last_load_ts,
        pulse_strln_dept_sale_uniq,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr,
        TailCashierNo AS pulse_strln_cshr_nbr,
        TailPosNo AS pulse_strln_rgstr_nbr,
        TailTicketNumber AS pulse_strln_tckt_nbr,
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt,
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm,
        TailSequenceNumber AS pulse_strln_seq_nbr,
        Opcode AS pulse_strln_oprtn_cd,
        (decimal)(Amount / 100) AS pulse_strln_dept_sale_amt,
        AtQty AS pulse_strln_dept_sale_at_qty,
        Cancel AS pulse_strln_dept_sale_cancel_ind,
        DepartmentNo AS pulse_strln_dept_sale_dept_nbr,
        FSpayment AS pulse_strln_dept_sale_fd_stmp_pmt_ind,
        ManualPrice AS pulse_strln_dept_sale_manl_prc_ind,
        Negative AS pulse_strln_dept_sale_neg_ind,
        (decimal)(NoTaxAmount / 100) AS pulse_strln_dept_sale_no_tax_amt,
        (decimal)(Price / 100) AS pulse_strln_dept_sale_prc,
        product_code AS pulse_strln_dept_sale_prdt_cd,
        Qty AS pulse_strln_dept_sale_qty,
        QtyIsWeight AS pulse_strln_dept_sale_qty_wgt_ind,
        RefundDpt AS pulse_strln_dept_sale_rfnd_dept,
        ReturnType AS pulse_strln_dept_sale_rtn_typ,
        StoreCoupon AS pulse_strln_dept_sale_store_cpn_ind,
        Subtract AS pulse_strln_dept_sale_subtrct_ind,
        tax_ptr AS pulse_strln_dept_sale_tax_pntr,
        VendorCoupon AS pulse_strln_dept_sale_vend_cpn_ind,
        WeightFromScale AS pulse_strln_dept_sale_wgt_from_scale_ind,
        opt_bottle_deposit AS pulse_strln_dept_sale_bottle_depst_ind,
        opt_bottle_refund AS pulse_strln_dept_sale_bottle_rfnd_ind,
        opt_enh_refund_bonus AS pulse_strln_dept_sale_enhcd_rfnd_bonus_ind,
        opt_non_merch AS pulse_strln_dept_sale_non_mdse_ind,
        opt_price_override AS pulse_strln_dept_sale_prc_ovrd_ind,
        opt_staff_discountable AS pulse_strln_dept_sale_staff_disc_ind,
        opt_was_cancelled AS pulse_strln_dept_sale_was_cancel_ind,
        TailBadRecord AS pulse_strln_bad_rcd_ind,
        TailDate AS pulse_strln_dt,
        TailTime AS pulse_strln_tm,
        TailDoNotProcess AS pulse_strln_do_not_proc_ind,
        TailPcNo AS pulse_strln_term_nbr,
        TailPosOffline AS pulse_strln_pos_offln_ind,
        TailReturnTicket AS pulse_strln_rtn_tckt,
        TailTV AS pulse_strln_tv,
        TailTrainingMode AS pulse_strln_train_mode_ind,
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Unique AS pulse_strln_dept_sale_uniq,
        Decimal.Parse(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_dept_sale;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_dept_sale" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_dept_sale
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_media 
DECLARE @pulse_strln_media_path string = string.Format("{0}/media.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_media_path) AND FILE.LENGTH(@pulse_strln_media_path) > 0 THEN
     @pulse_strln_media = 
        EXTRACT 
            Opcode int, 
            MediaNo int, 
            Change short, 
            Subtract short, 
            Cancel short, 
            opt_was_cancelled short, 
            Return int, 
            opt_MCR_used short, 
            CouponSale short, 
            CashBack short, 
            Type int, 
            Amount decimal, 
            AccountNumber string, 
            exp_date string, 
            auth_no string, 
            count int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_media_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

     ALTER TABLE pos.storeline.pulse_strln_media ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_media (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_media_acct_nbr, 
        pulse_strln_media_amt, 
        pulse_strln_media_auth_nbr, 
        pulse_strln_media_cancel_ind, 
        pulse_strln_media_cashbk_ind, 
        pulse_strln_media_chg_ind, 
        pulse_strln_media_cnt, 
        pulse_strln_media_cpn_sale_ind, 
        pulse_strln_media_xpir_dt, 
        pulse_strln_media_nbr, 
        pulse_strln_media_rtn, 
        pulse_strln_media_subtrct_ind, 
        pulse_strln_media_typ, 
        pulse_strln_media_mcr_used_ind, 
        pulse_strln_media_was_cancel_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_media_uniq,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        AccountNumber AS pulse_strln_media_acct_nbr, 
		(decimal)(Amount / 100) AS pulse_strln_media_amt, 
        auth_no AS pulse_strln_media_auth_nbr, 
        Cancel AS pulse_strln_media_cancel_ind, 
        CashBack AS pulse_strln_media_cashbk_ind, 
        Change AS pulse_strln_media_chg_ind, 
        count AS pulse_strln_media_cnt, 
        CouponSale AS pulse_strln_media_cpn_sale_ind, 
        exp_date AS pulse_strln_media_xpir_dt, 
        MediaNo AS pulse_strln_media_nbr, 
        Return AS pulse_strln_media_rtn, 
        Subtract AS pulse_strln_media_subtrct_ind, 
        Type AS pulse_strln_media_typ, 
        opt_MCR_used AS pulse_strln_media_mcr_used_ind, 
        opt_was_cancelled AS pulse_strln_media_was_cancel_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_media_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_media;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_media" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_media
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_media_extsn_rcd 
DECLARE @pulse_strln_media_extsn_rcd_path string = string.Format("{0}/mediaextrecord.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_media_extsn_rcd_path) AND FILE.LENGTH(@pulse_strln_media_extsn_rcd_path) > 0 THEN
    @pulse_strln_media_extsn_rcd = 
        EXTRACT 
            Opcode int,
            tender_req_add_fee short, 
            bank_account string, 
            check_number string, 
            Deposit short, 
            GiftCard short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_media_extsn_rcd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_media_extsn_rcd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_media_extsn_rcd (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_media_extsn_rcd_bank_acct, 
        pulse_strln_media_extsn_rcd_chk_nbr, 
        pulse_strln_media_extsn_rcd_depst_ind, 
        pulse_strln_media_extsn_rcd_gift_crd_ind, 
        pulse_strln_media_extsn_rcd_tnd_rqrd_add_fee_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        bank_account AS pulse_strln_media_extsn_rcd_bank_acct, 
        check_number AS pulse_strln_media_extsn_rcd_chk_nbr, 
        Deposit AS pulse_strln_media_extsn_rcd_depst_ind, 
        GiftCard AS pulse_strln_media_extsn_rcd_gift_crd_ind, 
        tender_req_add_fee AS pulse_strln_media_extsn_rcd_tnd_rqrd_add_fee_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_media_extsn_rcd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_media_extsn_rcd" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_media_extsn_rcd
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_media_extsn_rcd_2 
DECLARE @pulse_strln_media_extsn_rcd_2_path string = string.Format("{0}/mediaextrecord2.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_media_extsn_rcd_2_path) AND FILE.LENGTH(@pulse_strln_media_extsn_rcd_2_path) > 0 THEN
    @pulse_strln_media_extsn_rcd_2 = 
        EXTRACT 
            Opcode int,
            EFTRefNo string, 
            EnhMPRVoucherprt int, 
            PrtCashBal int, 
            PrtFSBal int, 
            void_tndr_not_allowed short, 
            VoucherTndr int, 
            Voucher int, 
            MediaExt3 short, 
            PrtGftCardBal int, 
            EFTTenderNo int, 
            SettlementDate string, 
            CardType int, 
            AccountType int, 
            TransType int, 
            SeqNo string, 
            EBTVoucherNumber string, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                            
        FROM @pulse_strln_media_extsn_rcd_2_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_media_extsn_rcd_2 ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_media_extsn_rcd_2 (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_media_extsn_rcd_2_acct_typ, 
        pulse_strln_media_extsn_rcd_2_crd_typ, 
        pulse_strln_media_extsn_rcd_2_ebt_vchr_nbr, 
        pulse_strln_media_extsn_rcd_2_eft_ref_nbr, 
        pulse_strln_media_extsn_rcd_2_eft_tnd_nbr, 
        pulse_strln_media_extsn_rcd_2_enhcd_mpr_vchr_prt, 
        pulse_strln_media_extsn_rcd_2_media_extsn_3_ind, 
        pulse_strln_media_extsn_rcd_2_prt_cash_bal, 
        pulse_strln_media_extsn_rcd_2_prt_fd_stmp_bal, 
        pulse_strln_media_extsn_rcd_2_prt_gift_crd_bal, 
        pulse_strln_media_extsn_rcd_2_seq_nbr, 
        pulse_strln_media_extsn_rcd_2_stlmnt_dt, 
        pulse_strln_media_extsn_rcd_2_trans_typ, 
        pulse_strln_media_extsn_rcd_2_vchr, 
        pulse_strln_media_extsn_rcd_2_vchr_tnd, 
        pulse_strln_media_extsn_rcd_2_void_tnd_not_alwd_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        AccountType AS pulse_strln_media_extsn_rcd_2_acct_typ, 
        CardType AS pulse_strln_media_extsn_rcd_2_crd_typ, 
        EBTVoucherNumber AS pulse_strln_media_extsn_rcd_2_ebt_vchr_nbr, 
        EFTRefNo AS pulse_strln_media_extsn_rcd_2_eft_ref_nbr, 
        EFTTenderNo AS pulse_strln_media_extsn_rcd_2_eft_tnd_nbr, 
        EnhMPRVoucherprt AS pulse_strln_media_extsn_rcd_2_enhcd_mpr_vchr_prt, 
        MediaExt3 AS pulse_strln_media_extsn_rcd_2_media_extsn_3_ind, 
        PrtCashBal AS pulse_strln_media_extsn_rcd_2_prt_cash_bal, 
        PrtFSBal AS pulse_strln_media_extsn_rcd_2_prt_fd_stmp_bal, 
        PrtGftCardBal AS pulse_strln_media_extsn_rcd_2_prt_gift_crd_bal, 
        SeqNo AS pulse_strln_media_extsn_rcd_2_seq_nbr, 
        SettlementDate AS pulse_strln_media_extsn_rcd_2_stlmnt_dt, 
        TransType AS pulse_strln_media_extsn_rcd_2_trans_typ, 
        Voucher AS pulse_strln_media_extsn_rcd_2_vchr, 
        VoucherTndr AS pulse_strln_media_extsn_rcd_2_vchr_tnd, 
        void_tndr_not_allowed AS pulse_strln_media_extsn_rcd_2_void_tnd_not_alwd_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_media_extsn_rcd_2;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_media_extsn_rcd_2" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_media_extsn_rcd_2
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_media_extsn_rcd_3 
DECLARE @pulse_strln_media_extsn_rcd_3_path string = string.Format("{0}/mediaextrecord3.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_media_extsn_rcd_3_path) AND FILE.LENGTH(@pulse_strln_media_extsn_rcd_3_path) > 0 THEN
    @pulse_strln_media_extsn_rcd_3 = 
        EXTRACT 
            Opcode int,
            EBTGenNo string, 
            EBTCaseNo string, 
            SigCapFailed short, 
            FleetCard short, 
            MediaExt4 short, 
            MediaExt5 short, 
            BiometricsEFTTranFlag short, 
            EFTCrdDataMasked short, 
            DispNegTndrAmtFlg short, 
            BarcodeTndrCpn int, 
            CashbackValue int, 
            DebitAcctType int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_media_extsn_rcd_3_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_media_extsn_rcd_3 ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_media_extsn_rcd_3 (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_media_extsn_rcd_3_barcd_tnd_cpn, 
        pulse_strln_media_extsn_rcd_3_biomtrc_eft_trans_ind, 
        pulse_strln_media_extsn_rcd_3_cash_back_val, 
        pulse_strln_media_extsn_rcd_3_dbt_acct_typ, 
        pulse_strln_media_extsn_rcd_3_disp_neg_tnd_amt_ind, 
        pulse_strln_media_extsn_rcd_3_ebt_case_nbr, 
        pulse_strln_media_extsn_rcd_3_ebt_gen_nbr, 
        pulse_strln_media_extsn_rcd_3_eft_crd_data_masked_ind, 
        pulse_strln_media_extsn_rcd_3_fleet_crd_ind, 
        pulse_strln_media_extsn_rcd_3_media_extsn_4_ind, 
        pulse_strln_media_extsn_rcd_3_media_extsn_5_ind, 
        pulse_strln_media_extsn_rcd_3_sgntr_captr_failed_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        BarcodeTndrCpn AS pulse_strln_media_extsn_rcd_3_barcd_tnd_cpn, 
        BiometricsEFTTranFlag AS pulse_strln_media_extsn_rcd_3_biomtrc_eft_trans_ind, 
        CashbackValue AS pulse_strln_media_extsn_rcd_3_cash_back_val, 
        DebitAcctType AS pulse_strln_media_extsn_rcd_3_dbt_acct_typ, 
        DispNegTndrAmtFlg AS pulse_strln_media_extsn_rcd_3_disp_neg_tnd_amt_ind, 
        EBTCaseNo AS pulse_strln_media_extsn_rcd_3_ebt_case_nbr, 
        EBTGenNo AS pulse_strln_media_extsn_rcd_3_ebt_gen_nbr, 
        EFTCrdDataMasked AS pulse_strln_media_extsn_rcd_3_eft_crd_data_masked_ind, 
        FleetCard AS pulse_strln_media_extsn_rcd_3_fleet_crd_ind, 
        MediaExt4 AS pulse_strln_media_extsn_rcd_3_media_extsn_4_ind, 
        MediaExt5 AS pulse_strln_media_extsn_rcd_3_media_extsn_5_ind, 
        SigCapFailed AS pulse_strln_media_extsn_rcd_3_sgntr_captr_failed_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_media_extsn_rcd_3;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_media_extsn_rcd_3" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_media_extsn_rcd_3
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_cpn 
DECLARE @pulse_strln_cpn_path string = string.Format("{0}/coupon.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cpn_path) AND FILE.LENGTH(@pulse_strln_cpn_path) > 0 THEN
    @pulse_strln_cpn = 
        EXTRACT 
            Opcode int,
            Subtract short, 
            Cancel short, 
            StoreCoupon short, 
            Vendorcoupon short, 
            BonusCoupon short, 
            UPC5coupon short, 
            opt_manual_entered_amount short, 
            opt_manual_entered_dept short, 
            Qty int, 
            Amount decimal, 
            TenderNbr int, 
            TenderType int, 
            CouponDept int, 
            CouponCode long, 
            coupon_name string, 
            tax_ptr int, 
            min_qty int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_cpn_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cpn ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cpn (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_cpn_amt, 
        pulse_strln_cpn_bonus_cpn_ind, 
        pulse_strln_cpn_cancel_ind, 
        pulse_strln_cpn_cd, 
        pulse_strln_cpn_dept, 
        pulse_strln_cpn_nam, 
        pulse_strln_cpn_min_qty, 
        pulse_strln_cpn_qty, 
        pulse_strln_cpn_store_cpn_ind, 
        pulse_strln_cpn_subtrct_ind, 
        pulse_strln_cpn_tax_pntr, 
        pulse_strln_cpn_tnd_numer, 
        pulse_strln_cpn_tnd_typ, 
        pulse_strln_cpn_upc5_cpn_ind, 
        pulse_strln_cpn_vend_cpn_ind, 
        pulse_strln_cpn_manl_entrd_amt_ind, 
        pulse_strln_cpn_manl_entrd_dept_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_cpn_uniq,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_cpn_amt, 
        BonusCoupon AS pulse_strln_cpn_bonus_cpn_ind, 
        Cancel AS pulse_strln_cpn_cancel_ind, 
        CouponCode AS pulse_strln_cpn_cd, 
        CouponDept AS pulse_strln_cpn_dept, 
        coupon_name AS pulse_strln_cpn_nam, 
        min_qty AS pulse_strln_cpn_min_qty, 
        Qty AS pulse_strln_cpn_qty, 
        StoreCoupon AS pulse_strln_cpn_store_cpn_ind, 
        Subtract AS pulse_strln_cpn_subtrct_ind, 
        tax_ptr AS pulse_strln_cpn_tax_pntr, 
        TenderNbr AS pulse_strln_cpn_tnd_numer, 
        TenderType AS pulse_strln_cpn_tnd_typ, 
        UPC5coupon AS pulse_strln_cpn_upc5_cpn_ind, 
        Vendorcoupon AS pulse_strln_cpn_vend_cpn_ind, 
        opt_manual_entered_amount AS pulse_strln_cpn_manl_entrd_amt_ind, 
        opt_manual_entered_dept AS pulse_strln_cpn_manl_entrd_dept_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_cpn_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cpn;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cpn" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cpn
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_cpn_info 
DECLARE @pulse_strln_cpn_info_path string = string.Format("{0}/couponinfo.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cpn_info_path) AND FILE.LENGTH(@pulse_strln_cpn_info_path) > 0 THEN
    @pulse_strln_cpn_info = 
        EXTRACT 
            Opcode int,
            Function int, 
            WgtCpn short, 
            DeptStoreCpn short, 
            DeptOtherCpn short, 
            DeptVendrCpn short, 
            PluStoreCpn short, 
            PluOtherCpn short, 
            PluVendrCpn short, 
            FreqShopperCpn short, 
            Weight int, 
            Price decimal, 
            Amount decimal, 
            ReturnType int, 
            TripleCpn short, 
            SmartCardCpn short, 
            PriceEmbedCpn short, 
            LongQty int, 
            PLUCode long, 
            ManualDatabarCoupon int, 
            DatabarCoupon short, 
            opt_was_cancelled short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string           
        FROM @pulse_strln_cpn_info_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cpn_info ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cpn_info (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_cpn_info_amt, 
        pulse_strln_cpn_info_data_bar_cpn_ind, 
        pulse_strln_cpn_info_dept_othr_cpn_ind, 
        pulse_strln_cpn_info_dept_store_cpn_ind, 
        pulse_strln_cpn_info_dept_vend_cpn_ind, 
        pulse_strln_cpn_info_freq_shop_cpn_ind, 
        pulse_strln_cpn_info_func, 
        pulse_strln_cpn_info_long_qty, 
        pulse_strln_cpn_info_manl_data_bar_cpn, 
        pulse_strln_cpn_info_plu_cd, 
        pulse_strln_cpn_info_plu_othr_cpn_ind, 
        pulse_strln_cpn_info_plu_store_cpn_ind, 
        pulse_strln_cpn_info_plu_vend_cpn_ind, 
        pulse_strln_cpn_info_prc, 
        pulse_strln_cpn_info_prc_embd_cpn_ind, 
        pulse_strln_cpn_info_rtn_typ, 
        pulse_strln_cpn_info_smart_crd_cpn_ind, 
        pulse_strln_cpn_info_triple_cpn_ind, 
        pulse_strln_cpn_info_wgt, 
        pulse_strln_cpn_info_wgt_cpn_ind, 
        pulse_strln_cpn_info_was_cancel_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_cpn_info_uniq,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_cpn_info_amt, 
        DatabarCoupon AS pulse_strln_cpn_info_data_bar_cpn_ind, 
        DeptOtherCpn AS pulse_strln_cpn_info_dept_othr_cpn_ind, 
        DeptStoreCpn AS pulse_strln_cpn_info_dept_store_cpn_ind, 
        DeptVendrCpn AS pulse_strln_cpn_info_dept_vend_cpn_ind, 
        FreqShopperCpn AS pulse_strln_cpn_info_freq_shop_cpn_ind, 
        Function AS pulse_strln_cpn_info_func, 
        LongQty AS pulse_strln_cpn_info_long_qty, 
        ManualDatabarCoupon AS pulse_strln_cpn_info_manl_data_bar_cpn, 
        PLUCode AS pulse_strln_cpn_info_plu_cd, 
        PluOtherCpn AS pulse_strln_cpn_info_plu_othr_cpn_ind, 
        PluStoreCpn AS pulse_strln_cpn_info_plu_store_cpn_ind, 
        PluVendrCpn AS pulse_strln_cpn_info_plu_vend_cpn_ind, 
        (decimal)(Price / 100) AS pulse_strln_cpn_info_prc, 
        PriceEmbedCpn AS pulse_strln_cpn_info_prc_embd_cpn_ind, 
        ReturnType AS pulse_strln_cpn_info_rtn_typ, 
        SmartCardCpn AS pulse_strln_cpn_info_smart_crd_cpn_ind, 
        TripleCpn AS pulse_strln_cpn_info_triple_cpn_ind, 
        Weight AS pulse_strln_cpn_info_wgt, 
        WgtCpn AS pulse_strln_cpn_info_wgt_cpn_ind, 
        opt_was_cancelled AS pulse_strln_cpn_info_was_cancel_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_cpn_info_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cpn_info;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cpn_info" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cpn_info
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_databar_cpn_3 
DECLARE @pulse_strln_databar_cpn_3_path string = string.Format("{0}/info2databarcoupon_3.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_databar_cpn_3_path) AND FILE.LENGTH(@pulse_strln_databar_cpn_3_path) > 0 THEN
    @pulse_strln_databar_cpn_3 = 
        EXTRACT 
            Opcode int,
            Function int, 
            SubOpcode int, 
            CompanyPrefix string, 
            FamilyCode int, 
            PurchRequirement short, 
            PurchRequirementCode short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_databar_cpn_3_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_databar_cpn_3 ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_databar_cpn_3 (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_databar_cpn_3_func, 
        pulse_strln_databar_cpn_3_sub_oprtn_cd, 
        pulse_strln_databar_cpn_3_co_prefix, 
        pulse_strln_databar_cpn_3_fam_cd, 
        pulse_strln_databar_cpn_3_pur_req_ind, 
        pulse_strln_databar_cpn_3_pur_req_cd_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_databar_cpn_3_func, 
        SubOpcode AS pulse_strln_databar_cpn_3_sub_oprtn_cd, 
        CompanyPrefix AS pulse_strln_databar_cpn_3_co_prefix, 
        FamilyCode AS pulse_strln_databar_cpn_3_fam_cd, 
        PurchRequirement AS pulse_strln_databar_cpn_3_pur_req_ind, 
        PurchRequirementCode AS pulse_strln_databar_cpn_3_pur_req_cd_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_databar_cpn_3;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_databar_cpn_3" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_databar_cpn_3
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_databar_cpn_6 
DECLARE @pulse_strln_databar_cpn_6_path string = string.Format("{0}/info2databarcoupon_6.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_databar_cpn_6_path) AND FILE.LENGTH(@pulse_strln_databar_cpn_6_path) > 0 THEN
    @pulse_strln_databar_cpn_6 = 
        EXTRACT 
            Opcode int,
            Function int, 
            SubOpcode int, 
            Amount decimal, 
            Qty int, 
            OfferCode string, 
            SaveValue int, 
            AdditionalPurchaseRulesCode int, 
            ExpirationDate string, 
            StartDate string, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_databar_cpn_6_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_databar_cpn_6 ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_databar_cpn_6 (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_databar_cpn_6_func, 
        pulse_strln_databar_cpn_6_sub_oprtn_cd, 
        pulse_strln_databar_cpn_6_amt, 
        pulse_strln_databar_cpn_6_qty, 
        pulse_strln_databar_cpn_6_offer_cd, 
        pulse_strln_databar_cpn_6_save_val, 
        pulse_strln_databar_cpn_6_addl_pur_rules_cd, 
        pulse_strln_databar_cpn_6_xpir_dt, 
        pulse_strln_databar_cpn_6_strt_dt, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_databar_cpn_6_func, 
        SubOpcode AS pulse_strln_databar_cpn_6_sub_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_databar_cpn_6_amt, 
        Qty AS pulse_strln_databar_cpn_6_qty, 
        OfferCode AS pulse_strln_databar_cpn_6_offer_cd, 
        SaveValue AS pulse_strln_databar_cpn_6_save_val, 
        AdditionalPurchaseRulesCode AS pulse_strln_databar_cpn_6_addl_pur_rules_cd, 
        ExpirationDate AS pulse_strln_databar_cpn_6_xpir_dt, 
        StartDate AS pulse_strln_databar_cpn_6_strt_dt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_databar_cpn_6;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_databar_cpn_6" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_databar_cpn_6
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_club_crd 
DECLARE @pulse_strln_club_crd_path string = string.Format("{0}/clubcard.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_club_crd_path) AND FILE.LENGTH(@pulse_strln_club_crd_path) > 0 THEN
    @pulse_strln_club_crd = 
        EXTRACT 
            Opcode int, 
            Function int, 
            CardAccepted short, 
            CardVoided short, 
            MCRUsed short, 
            StaffDiscountCard short, 
            QualifySpendInfo int, 
            ClubCardReentry short, 
            FrequentShopperAccepted int, 
            ProcessedLateSwipe short, 
            SchemeNo int, 
            CardNo string, 
            TenderNo int, 
            CustomerType int, 
            CustomerPointsToDate int, 
            CustomerRedemptionValue int, 
            CustomerUpdateDate DateTime, 
            CardNoIncludesCD short, 
            QualifySpent int, 
            CRNG_NO int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_club_crd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_club_crd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_club_crd (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_club_crd_func, 
        pulse_strln_club_crd_crd_accp_ind, 
        pulse_strln_club_crd_crd_void_ind, 
        pulse_strln_club_crd_mcr_used_ind, 
        pulse_strln_club_crd_staff_disc_crd_ind, 
        pulse_strln_club_crd_qfy_spent, 
        pulse_strln_club_crd_qfy_spend_info, 
        pulse_strln_club_crd_freq_shop_accp, 
        pulse_strln_club_crd_nbr, 
        pulse_strln_club_crd_reentry_ind, 
        pulse_strln_club_crd_nbr_incld_cd_ind, 
        pulse_strln_club_crd_tnd_nbr, 
        pulse_strln_club_crd_scheme_nbr, 
        pulse_strln_club_crd_proc_late_swipe_ind, 
        pulse_strln_club_crd_cust_typ, 
        pulse_strln_club_crd_cust_redm_val, 
        pulse_strln_club_crd_cust_pnt_to_dt, 
        pulse_strln_club_crd_cust_upd_dt, 
        pulse_strln_club_crd_rng_nbr, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_club_crd_func, 
        CardAccepted AS pulse_strln_club_crd_crd_accp_ind, 
        CardVoided AS pulse_strln_club_crd_crd_void_ind, 
        MCRUsed AS pulse_strln_club_crd_mcr_used_ind, 
        StaffDiscountCard AS pulse_strln_club_crd_staff_disc_crd_ind, 
        QualifySpent AS pulse_strln_club_crd_qfy_spent, 
        QualifySpendInfo AS pulse_strln_club_crd_qfy_spend_info, 
        FrequentShopperAccepted AS pulse_strln_club_crd_freq_shop_accp, 
        Int64.Parse(CardNo == "" ? "0" : CardNo) AS pulse_strln_club_crd_nbr, 
        ClubCardReentry AS pulse_strln_club_crd_reentry_ind, 
        CardNoIncludesCD AS pulse_strln_club_crd_nbr_incld_cd_ind, 
        TenderNo AS pulse_strln_club_crd_tnd_nbr, 
        SchemeNo AS pulse_strln_club_crd_scheme_nbr, 
        ProcessedLateSwipe AS pulse_strln_club_crd_proc_late_swipe_ind, 
        CustomerType AS pulse_strln_club_crd_cust_typ, 
        CustomerRedemptionValue AS pulse_strln_club_crd_cust_redm_val, 
        CustomerPointsToDate AS pulse_strln_club_crd_cust_pnt_to_dt, 
        CustomerUpdateDate AS pulse_strln_club_crd_cust_upd_dt, 
        CRNG_NO AS pulse_strln_club_crd_rng_nbr, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_club_crd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_club_crd" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_club_crd
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_freq_shop 
DECLARE @pulse_strln_freq_shop_path string = string.Format("{0}/frequentshopper.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_freq_shop_path) AND FILE.LENGTH(@pulse_strln_freq_shop_path) > 0 THEN
    @pulse_strln_freq_shop = 
        EXTRACT 
            Opcode int, 
            Function int, 
            PluNo long, 
            RewardCheckUsed short, 
            OldPrice decimal, 
            NewPrice decimal, 
            Qty int, 
            SaveAmount decimal, 
            ShortDept int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_freq_shop_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_freq_shop ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_freq_shop (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_freq_shop_func, 
        pulse_strln_freq_shop_plu_nbr, 
        pulse_strln_freq_shop_rwrd_chk_used_ind, 
        pulse_strln_freq_shop_old_prc, 
        pulse_strln_freq_shop_new_prc, 
        pulse_strln_freq_shop_qty, 
        pulse_strln_freq_shop_save_amt, 
        pulse_strln_freq_shop_shrt_dept, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_freq_shop_func, 
        PluNo AS pulse_strln_freq_shop_plu_nbr, 
        RewardCheckUsed AS pulse_strln_freq_shop_rwrd_chk_used_ind, 
        (decimal)(OldPrice / 100) AS pulse_strln_freq_shop_old_prc, 
        (decimal)(NewPrice / 100) AS pulse_strln_freq_shop_new_prc, 
        Qty AS pulse_strln_freq_shop_qty, 
        (decimal)(SaveAmount / 100) AS pulse_strln_freq_shop_save_amt, 
        ShortDept AS pulse_strln_freq_shop_shrt_dept, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_freq_shop;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_freq_shop" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_freq_shop
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_promo_info 
DECLARE @pulse_strln_promo_info_path string = string.Format("{0}/promotioninfo.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_promo_info_path) AND FILE.LENGTH(@pulse_strln_promo_info_path) > 0 THEN
    @pulse_strln_promo_info = 
        EXTRACT 
            Opcode int,
            Function int, 
            DiscountedItem long, 
            BktNbr int, 
            CopientOfferNumber int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_promo_info_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_promo_info ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_promo_info (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_promo_info_bckt_nbr, 
        pulse_strln_promo_info_copient_offer_nbr, 
        pulse_strln_promo_info_disc_item, 
        pulse_strln_promo_info_func, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_promo_info_uniq,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        BktNbr AS pulse_strln_promo_info_bckt_nbr, 
        CopientOfferNumber AS pulse_strln_promo_info_copient_offer_nbr, 
        DiscountedItem AS pulse_strln_promo_info_disc_item, 
        Function AS pulse_strln_promo_info_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_promo_info_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_promo_info; 

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_promo_info" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_promo_info
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_info_promo_extsn 
DECLARE @pulse_strln_info_promo_extsn_path string = string.Format("{0}/info2promotionext.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_info_promo_extsn_path) AND FILE.LENGTH(@pulse_strln_info_promo_extsn_path) > 0 THEN
    @pulse_strln_info_promo_extsn = 
        EXTRACT 
            Opcode int,
            Function int, 
            Description string, 
            PromotionComplexity int, 
            ItemAlreadyTriggered short, 
            TriggeredPromotion short, 
            CashierInvoked int, 
            Otc int?, 
            DiscountAllocationPrinting int, 
            NewQuantity int, 
            PluDeptAmount decimal, 
            FundingMethod int, 
            WeightDecimalValue int, 
            ORScheme short, 
            ApportionmentAlgorithm int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_info_promo_extsn_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_info_promo_extsn ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_info_promo_extsn (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_seq_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_oprtn_cd, 
        pulse_strln_info_promo_extsn_apprtmnt_algm, 
        pulse_strln_info_promo_extsn_cshr_invoked, 
        pulse_strln_info_promo_extsn_dsc, 
        pulse_strln_info_promo_extsn_disc_allct_prtng, 
        pulse_strln_info_promo_extsn_func, 
        pulse_strln_info_promo_extsn_fund_mthd, 
        pulse_strln_info_promo_extsn_item_alrdy_trgr_ind, 
        pulse_strln_info_promo_extsn_new_qty, 
        pulse_strln_info_promo_extsn_or_scheme_ind, 
        pulse_strln_info_promo_extsn_otc, 
        pulse_strln_info_promo_extsn_plu_dept_amt, 
        pulse_strln_info_promo_extsn_promo_complexity, 
        pulse_strln_info_promo_extsn_trgr_promo_ind, 
        pulse_strln_info_promo_extsn_wgt_dcml_val, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        Opcode AS pulse_strln_oprtn_cd, 
        ApportionmentAlgorithm AS pulse_strln_info_promo_extsn_apprtmnt_algm, 
        CashierInvoked AS pulse_strln_info_promo_extsn_cshr_invoked, 
        Description AS pulse_strln_info_promo_extsn_dsc, 
        DiscountAllocationPrinting AS pulse_strln_info_promo_extsn_disc_allct_prtng, 
        Function AS pulse_strln_info_promo_extsn_func, 
        FundingMethod AS pulse_strln_info_promo_extsn_fund_mthd, 
        ItemAlreadyTriggered AS pulse_strln_info_promo_extsn_item_alrdy_trgr_ind, 
        NewQuantity AS pulse_strln_info_promo_extsn_new_qty, 
        ORScheme AS pulse_strln_info_promo_extsn_or_scheme_ind, 
        (Otc ?? 0) AS pulse_strln_info_promo_extsn_otc, 
        (decimal)(PluDeptAmount / 100) AS pulse_strln_info_promo_extsn_plu_dept_amt, 
        PromotionComplexity AS pulse_strln_info_promo_extsn_promo_complexity, 
        TriggeredPromotion AS pulse_strln_info_promo_extsn_trgr_promo_ind, 
        WeightDecimalValue AS pulse_strln_info_promo_extsn_wgt_dcml_val, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_info_promo_extsn;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_info_promo_extsn" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_info_promo_extsn
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_info_promo_extsn_2 
DECLARE @pulse_strln_info_promo_extsn_2_path string = string.Format("{0}/info2promotionext2.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_info_promo_extsn_2_path) AND FILE.LENGTH(@pulse_strln_info_promo_extsn_2_path) > 0 THEN
    @pulse_strln_info_promo_extsn_2 = 
        EXTRACT 
            Opcode int,
            Function int, 
            OrganizedReceiptSchemeTemplat int, 
            LastTriggeredPromotion int, 
            ConfigRewardAmount long, 
            DecimalPlaceInReward int, 
            ExternalRefID int, 
            TransSequenceAttachment long, 
            CopientCategoryID int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_info_promo_extsn_2_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_info_promo_extsn_2 ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_info_promo_extsn_2 (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_info_promo_extsn_2_func, 
        pulse_strln_info_promo_extsn_2_org_rcpt_scheme_tmplt, 
        pulse_strln_info_promo_extsn_2_last_trgr_promo, 
        pulse_strln_info_promo_extsn_2_confg_rwrd_amt, 
        pulse_strln_info_promo_extsn_2_copient_catg_id, 
        pulse_strln_info_promo_extsn_2_dcml_place_in_rwrd, 
        pulse_strln_info_promo_extsn_2_extl_ref_id, 
        pulse_strln_info_promo_extsn_2_trans_seq_atchmnt, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_info_promo_extsn_2_func, 
        OrganizedReceiptSchemeTemplat AS pulse_strln_info_promo_extsn_2_org_rcpt_scheme_tmplt, 
        LastTriggeredPromotion AS pulse_strln_info_promo_extsn_2_last_trgr_promo, 
        ConfigRewardAmount AS pulse_strln_info_promo_extsn_2_confg_rwrd_amt, 
        CopientCategoryID AS pulse_strln_info_promo_extsn_2_copient_catg_id, 
        DecimalPlaceInReward AS pulse_strln_info_promo_extsn_2_dcml_place_in_rwrd, 
        ExternalRefID AS pulse_strln_info_promo_extsn_2_extl_ref_id, 
        TransSequenceAttachment AS pulse_strln_info_promo_extsn_2_trans_seq_atchmnt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_info_promo_extsn_2;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_info_promo_extsn_2" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_info_promo_extsn_2
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_disc 
DECLARE @pulse_strln_disc_path string = string.Format("{0}/discount.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_disc_path) AND FILE.LENGTH(@pulse_strln_disc_path) > 0 THEN
    @pulse_strln_disc = 
        EXTRACT 
            Opcode int, 
            ItemCode long, 
            Dept int, 
            Subtract short, 
            Cancel short, 
            Negative short, 
            Manual int, 
            PercentFlag short, 
            FSPayment short, 
            Promotion short, 
            Reduction short, 
            Offer short, 
            MemberDiscount short, 
            DiscountType int, 
            Percent int, 
            ReturnType int, 
            TaxPtr int, 
            Qty int, 
            Price decimal, 
            Amount decimal, 
            Msu int?, 
            Tender int, 
            NoTaxAmount decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_disc_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_disc ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_disc (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_disc_amt, 
        pulse_strln_disc_cancel_ind, 
        pulse_strln_disc_dept, 
        pulse_strln_disc_disc_typ, 
        pulse_strln_disc_fd_stmp_pmt_ind, 
        pulse_strln_disc_item_cd, 
        pulse_strln_disc_msu, 
        pulse_strln_disc_manl, 
        pulse_strln_disc_mbr_disc_ind, 
        pulse_strln_disc_neg_ind, 
        pulse_strln_disc_no_tax_amt, 
        pulse_strln_disc_offer_ind, 
        pulse_strln_disc_pct, 
        pulse_strln_disc_pct_ind, 
        pulse_strln_disc_prc, 
        pulse_strln_disc_promo_ind, 
        pulse_strln_disc_qty, 
        pulse_strln_disc_rdctn_ind, 
        pulse_strln_disc_rtn_typ, 
        pulse_strln_disc_subtrct_ind, 
        pulse_strln_disc_tax_pntr, 
        pulse_strln_disc_tnd, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_disc_uniq,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_disc_amt, 
        Cancel AS pulse_strln_disc_cancel_ind, 
        Dept AS pulse_strln_disc_dept, 
        DiscountType AS pulse_strln_disc_disc_typ, 
        FSPayment AS pulse_strln_disc_fd_stmp_pmt_ind, 
        ItemCode AS pulse_strln_disc_item_cd, 
        (Msu ?? 0) AS pulse_strln_disc_msu, 
        Manual AS pulse_strln_disc_manl, 
        MemberDiscount AS pulse_strln_disc_mbr_disc_ind, 
        Negative AS pulse_strln_disc_neg_ind, 
        (decimal)(NoTaxAmount / 100) AS pulse_strln_disc_no_tax_amt, 
        Offer AS pulse_strln_disc_offer_ind, 
        Percent AS pulse_strln_disc_pct, 
        PercentFlag AS pulse_strln_disc_pct_ind, 
        (decimal)(Price / 100) AS pulse_strln_disc_prc, 
        Promotion AS pulse_strln_disc_promo_ind, 
        Qty AS pulse_strln_disc_qty, 
        Reduction AS pulse_strln_disc_rdctn_ind, 
        ReturnType AS pulse_strln_disc_rtn_typ, 
        Subtract AS pulse_strln_disc_subtrct_ind, 
        TaxPtr AS pulse_strln_disc_tax_pntr, 
        Tender AS pulse_strln_disc_tnd, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_disc_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_disc;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_disc" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_disc
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_prc_ovrd 
DECLARE @pulse_strln_prc_ovrd_path string = string.Format("{0}/priceoverride.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_prc_ovrd_path) AND FILE.LENGTH(@pulse_strln_prc_ovrd_path) > 0 THEN
    @pulse_strln_prc_ovrd = 
        EXTRACT 
            Opcode int,
            Function int, 
            ElectronicReduced short, 
            ForcePrice short, 
            OptSubtract short, 
            OptCancel short, 
            QtyIfFuelGallons int, 
            QtyIsWeight short, 
            QtyIsDec short, 
            org_price decimal, 
            ReducedPrice decimal, 
            Qty int, 
            AmountDiff decimal, 
            Upc long?, 
            type int, 
            reason_no int, 
            RtnType int, 
            org_amount decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_prc_ovrd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_prc_ovrd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_prc_ovrd (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_prc_ovrd_amt_diff, 
        pulse_strln_prc_ovrd_elect_rdcd_ind, 
        pulse_strln_prc_ovrd_frce_prc_ind, 
        pulse_strln_prc_ovrd_func, 
        pulse_strln_prc_ovrd_optnl_cancel_ind, 
        pulse_strln_prc_ovrd_optnl_subtrct_ind, 
        pulse_strln_prc_ovrd_qty, 
        pulse_strln_prc_ovrd_qty_fuel_gln, 
        pulse_strln_prc_ovrd_qty_dcml_ind, 
        pulse_strln_prc_ovrd_qty_wgt_ind, 
        pulse_strln_prc_ovrd_rdcd_prc, 
        pulse_strln_prc_ovrd_rtn_typ, 
        pulse_strln_prc_ovrd_upc_nbr, 
        pulse_strln_prc_ovrd_orig_amt, 
        pulse_strln_prc_ovrd_orig_prc, 
        pulse_strln_prc_ovrd_rsn_nbr, 
        pulse_strln_prc_ovrd_typ, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(AmountDiff / 100) AS pulse_strln_prc_ovrd_amt_diff, 
        ElectronicReduced AS pulse_strln_prc_ovrd_elect_rdcd_ind, 
        ForcePrice AS pulse_strln_prc_ovrd_frce_prc_ind, 
        Function AS pulse_strln_prc_ovrd_func, 
        OptCancel AS pulse_strln_prc_ovrd_optnl_cancel_ind, 
        OptSubtract AS pulse_strln_prc_ovrd_optnl_subtrct_ind, 
        Qty AS pulse_strln_prc_ovrd_qty, 
        QtyIfFuelGallons AS pulse_strln_prc_ovrd_qty_fuel_gln, 
        QtyIsDec AS pulse_strln_prc_ovrd_qty_dcml_ind, 
        QtyIsWeight AS pulse_strln_prc_ovrd_qty_wgt_ind, 
        (decimal)(ReducedPrice / 100) AS pulse_strln_prc_ovrd_rdcd_prc, 
        RtnType AS pulse_strln_prc_ovrd_rtn_typ, 
        (long)(Upc ?? 0) AS pulse_strln_prc_ovrd_upc_nbr, 
        (decimal)(org_amount / 100) AS pulse_strln_prc_ovrd_orig_amt, 
        (decimal)(org_price / 100) AS pulse_strln_prc_ovrd_orig_prc, 
        reason_no AS pulse_strln_prc_ovrd_rsn_nbr, 
        type AS pulse_strln_prc_ovrd_typ, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_prc_ovrd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_prc_ovrd" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_prc_ovrd
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_tax 
DECLARE @pulse_strln_tax_path string = string.Format("{0}/tax.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_tax_path) AND FILE.LENGTH(@pulse_strln_tax_path) > 0 THEN
    @pulse_strln_tax = 
        EXTRACT 
            Opcode int,
            taxnumber int, 
            taxable_amt decimal, 
            tax_amt decimal, 
            exempt short, 
            TaxIncl int, 
            TaxExemptNo string, 
            FdStmpFrgvTxbl decimal, 
            FdStmpFrgvTx decimal, 
            NoTaxAmunt int, 
            TaxRefundAmount decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_tax_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_tax ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_tax (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_tax_exmpt_ind,
	    pulse_strln_tax_fd_stmp_tax,
	    pulse_strln_tax_fd_stmp_txbl,
	    pulse_strln_tax_no_tax_amt,
	    pulse_strln_txbl_amt,
	    pulse_strln_tax_exmpt_nbr,
	    pulse_strln_tax_incld,
	    pulse_strln_tax_nbr,
	    pulse_strln_tax_rfnd_amt,
	    pulse_strln_tax_amt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )  
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
	    !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        exempt AS pulse_strln_tax_exmpt_ind, 
        (decimal)(FdStmpFrgvTx / 100) AS pulse_strln_tax_fd_stmp_tax, 
        (decimal)(FdStmpFrgvTxbl / 100) AS pulse_strln_tax_fd_stmp_txbl, 
        NoTaxAmunt AS pulse_strln_tax_no_tax_amt, 
        (decimal)(taxable_amt / 100) AS pulse_strln_txbl_amt, 
	    TaxExemptNo AS pulse_strln_tax_exmpt_nbr, 
        TaxIncl AS pulse_strln_tax_incld, 
        taxnumber AS pulse_strln_tax_nbr, 
        (decimal)(TaxRefundAmount / 100) AS pulse_strln_tax_rfnd_amt, 
        (decimal)(tax_amt / 100) AS pulse_strln_tax_amt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
	    TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_tax;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_tax" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_tax
    GROUP BY TicketStart_DT ?? TailDate;
END;


// pulse_strln_tax_exmpt 
DECLARE @pulse_strln_tax_exmpt_path string = string.Format("{0}/taxexempt.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_tax_exmpt_path) AND FILE.LENGTH(@pulse_strln_tax_exmpt_path) > 0 THEN
    @pulse_strln_tax_exmpt = 
        EXTRACT 
            Opcode int,
            Function int, 
            Options int, 
            ExemptNo string, 
            AllTax int, 
            Tax int, 
            WICTaxExempt short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_tax_exmpt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_tax_exmpt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_tax_exmpt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_tax_exmpt_all_tax,
	    pulse_strln_tax_exmpt_nbr,
	    pulse_strln_tax_exmpt_func,
	    pulse_strln_tax_exmpt_options,
	    pulse_strln_tax_exmpt_tax,
	    pulse_strln_tax_exmpt_wic_tax_exmpt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        AllTax AS pulse_strln_tax_exmpt_all_tax, 
        ExemptNo AS pulse_strln_tax_exmpt_nbr, 
        Function AS pulse_strln_tax_exmpt_func, 
        Options AS pulse_strln_tax_exmpt_options, 
        Tax AS pulse_strln_tax_exmpt_tax, 
        WICTaxExempt AS pulse_strln_tax_exmpt_wic_tax_exmpt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_tax_exmpt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_tax_exmpt" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_tax_exmpt
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_gift_crd_actvtn 
DECLARE @pulse_strln_gift_crd_actvtn_path string = string.Format("{0}/giftcardactivation.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_gift_crd_actvtn_path) AND FILE.LENGTH(@pulse_strln_gift_crd_actvtn_path) > 0 THEN
    @pulse_strln_gift_crd_actvtn = 
        EXTRACT 
            Opcode int,
            Function int, 
            ActionType int, 
            CardNumber string, 
            AuthNumber string, 
            ReferenceNumber string, 
            BeginBalance int, 
            TransactionAmount long,
            Activation short, 
            Recharge short, 
            VariableCard short, 
            ManualSwipe short, 
            OptExtData short, 
            AllowDeactivation short, 
            Preactivated short, 
            ActivationAfterTender short, 
            TenderNo int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string                        
        FROM @pulse_strln_gift_crd_actvtn_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_gift_crd_actvtn ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_gift_crd_actvtn (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_gift_crd_actvtn_func,
	    pulse_strln_gift_crd_actvtn_actn_typ,
	    pulse_strln_gift_crd_actvtn_crd_nbr,
	    pulse_strln_gift_crd_actvtn_auth_nbr,
	    pulse_strln_gift_crd_actvtn_ref_nbr,
	    pulse_strln_gift_crd_actvtn_bgn_bal,
	    pulse_strln_gift_crd_actvtn_trans_amt,
	    pulse_strln_gift_crd_actvtn_ind,
	    pulse_strln_gift_crd_actvtn_rchrg_ind,
	    pulse_strln_gift_crd_actvtn_var_crd_ind,
	    pulse_strln_gift_crd_actvtn_manl_swipe_ind,
	    pulse_strln_gift_crd_actvtn_optext_data_ind,
	    pulse_strln_gift_crd_alwd_actvtn_ind,
	    pulse_strln_gift_crd_pre_actvd_ind,
	    pulse_strln_gift_crd_actvtn_aftr_tnd_ind,
	    pulse_strln_gift_crd_actvtn_tnd_nbr,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_gift_crd_actvtn_func, 
        ActionType AS pulse_strln_gift_crd_actvtn_actn_typ, 
        CardNumber AS pulse_strln_gift_crd_actvtn_crd_nbr, 
        AuthNumber AS pulse_strln_gift_crd_actvtn_auth_nbr, 
        ReferenceNumber AS pulse_strln_gift_crd_actvtn_ref_nbr, 
        BeginBalance AS pulse_strln_gift_crd_actvtn_bgn_bal, 
        (decimal)(TransactionAmount / 100) AS pulse_strln_gift_crd_actvtn_trans_amt, 
        Activation AS pulse_strln_gift_crd_actvtn_ind, 
        Recharge AS pulse_strln_gift_crd_actvtn_rchrg_ind, 
        VariableCard AS pulse_strln_gift_crd_actvtn_var_crd_ind, 
        ManualSwipe AS pulse_strln_gift_crd_actvtn_manl_swipe_ind, 
        OptExtData AS pulse_strln_gift_crd_actvtn_optext_data_ind, 
        AllowDeactivation AS pulse_strln_gift_crd_alwd_actvtn_ind, 
        Preactivated AS pulse_strln_gift_crd_pre_actvd_ind, 
        ActivationAfterTender AS pulse_strln_gift_crd_actvtn_aftr_tnd_ind, 
        TenderNo AS pulse_strln_gift_crd_actvtn_tnd_nbr, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_gift_crd_actvtn;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_gift_crd_actvtn" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_gift_crd_actvtn
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_gift_crd_actvtn_extsn 
DECLARE @pulse_strln_gift_crd_actvtn_extsn_path string = string.Format("{0}/giftcardactivationextended.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_gift_crd_actvtn_extsn_path) AND FILE.LENGTH(@pulse_strln_gift_crd_actvtn_extsn_path) > 0 THEN
    @pulse_strln_gift_crd_actvtn_extsn = 
        EXTRACT 
            Opcode int,
            Function int, 
            Template string, 
            ActivationType int, 
            BarcodeDescription string, 
            SequenceNo string, 
            Index int, 
            DelayedCardActivation short, 
            AccountLength16 short, 
            AccountLength19 short, 
            PrintBalance short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_gift_crd_actvtn_extsn_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_gift_crd_actvtn_extsn ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_gift_crd_actvtn_extsn (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_gift_crd_actvtn_extsn_func,
	    pulse_strln_gift_crd_actvtn_extsn_prt_bal_ind,
	    pulse_strln_gift_crd_actvtn_extsn_acct_long_length_ind,
	    pulse_strln_gift_crd_actvtn_extsn_acct_shrt_length_ind,
	    pulse_strln_gift_crd_actvtn_extsn_dlay_crd_actvtn_ind,
	    pulse_strln_gift_crd_actvtn_extsn_idx,
	    pulse_strln_gift_crd_actvtn_extsn_seq_nbr,
	    pulse_strln_gift_crd_actvtn_extsn_barcd_dsc,
	    pulse_strln_gift_crd_actvtn_extsn_actvtn_typ,
	    pulse_strln_gift_crd_actvtn_extsn_tmplt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_gift_crd_actvtn_extsn_func, 
        PrintBalance AS pulse_strln_gift_crd_actvtn_extsn_prt_bal_ind, 
        AccountLength19 AS pulse_strln_gift_crd_actvtn_extsn_acct_long_length_ind, 
        AccountLength16 AS pulse_strln_gift_crd_actvtn_extsn_acct_shrt_length_ind, 
        DelayedCardActivation AS pulse_strln_gift_crd_actvtn_extsn_dlay_crd_actvtn_ind, 
        Index AS pulse_strln_gift_crd_actvtn_extsn_idx, 
        SequenceNo AS pulse_strln_gift_crd_actvtn_extsn_seq_nbr, 
        BarcodeDescription AS pulse_strln_gift_crd_actvtn_extsn_barcd_dsc, 
        ActivationType AS pulse_strln_gift_crd_actvtn_extsn_actvtn_typ, 
        Template AS pulse_strln_gift_crd_actvtn_extsn_tmplt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_gift_crd_actvtn_extsn;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_gift_crd_actvtn_extsn" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_gift_crd_actvtn_extsn
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_advc_freq_shop 
DECLARE @pulse_strln_advc_freq_shop_path string = string.Format("{0}/advancedfrequentshopper.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_advc_freq_shop_path) AND FILE.LENGTH(@pulse_strln_advc_freq_shop_path) > 0 THEN
    @pulse_strln_advc_freq_shop = 
        EXTRACT 
            Opcode int,
            Function int, 
            Code long, 
            ExceedLimit int, 
            OldPrice decimal, 
            NewPrice decimal, 
            Quantity int, 
            SaveAmount decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_advc_freq_shop_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_advc_freq_shop ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_advc_freq_shop (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_advc_freq_shop_oprtn_cd,
	    pulse_strln_advc_freq_shop_func,
	    pulse_strln_advc_freq_shop_cd,
	    pulse_strln_advc_freq_shop_exceed_lmt,
	    pulse_strln_advc_freq_shop_qty,
	    pulse_strln_advc_freq_shop_old_prc,
	    pulse_strln_advc_freq_shop_new_prc,
	    pulse_strln_advc_freq_shop_save_amt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_advc_freq_shop_oprtn_cd, 
        Function AS pulse_strln_advc_freq_shop_func, 
        Code AS pulse_strln_advc_freq_shop_cd, 
        ExceedLimit AS pulse_strln_advc_freq_shop_exceed_lmt, 
        Quantity AS pulse_strln_advc_freq_shop_qty, 
        (decimal)(OldPrice / 100) AS pulse_strln_advc_freq_shop_old_prc, 
        (decimal)(NewPrice / 100) AS pulse_strln_advc_freq_shop_new_prc, 
        (decimal)(SaveAmount / 100) AS pulse_strln_advc_freq_shop_save_amt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_advc_freq_shop;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_advc_freq_shop" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_advc_freq_shop
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_fsa_sale_info 
DECLARE @pulse_strln_fsa_sale_info_path string = string.Format("{0}/info2_fsa_sale.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_fsa_sale_info_path) AND FILE.LENGTH(@pulse_strln_fsa_sale_info_path) > 0 THEN
    @pulse_strln_fsa_sale_info = 
        EXTRACT 
            Opcode int,
            Function int, 
            RXAmount decimal, 
            NonRXAmount decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_fsa_sale_info_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_fsa_sale_info ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_fsa_sale_info (
        pulse_strln_store_nbr,
        pulse_strln_cshr_nbr,
        pulse_strln_rgstr_nbr,
        pulse_strln_tckt_nbr,
        pulse_strln_tckt_dt,
        pulse_strln_tckt_tm,
        pulse_strln_seq_nbr,
        pulse_strln_oprtn_cd,
        pulse_strln_fsa_sale_info_func,
        pulse_strln_fsa_sale_info_non_rx_amt,
        pulse_strln_fsa_sale_info_phar_amt,
        pulse_strln_bad_rcd_ind,
        pulse_strln_dt,
        pulse_strln_tm,
        pulse_strln_do_not_proc_ind,
        pulse_strln_term_nbr,
        pulse_strln_pos_offln_ind,
        pulse_strln_rtn_tckt,
        pulse_strln_tv,
        pulse_strln_train_mode_ind,
        pulse_strln_void_tckt_ind,
        last_load_ts,
        transaction_id
    )                           
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_fsa_sale_info_func, 
        (decimal)(NonRXAmount / 100) AS pulse_strln_fsa_sale_info_non_rx_amt, 
        (decimal)(RXAmount / 100) AS pulse_strln_fsa_sale_info_phar_amt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_fsa_sale_info;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_fsa_sale_info" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_fsa_sale_info
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_info_prc_inqry 
DECLARE @pulse_strln_info_prc_inqry_path string = string.Format("{0}/infopriceinquiry.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_info_prc_inqry_path) AND FILE.LENGTH(@pulse_strln_info_prc_inqry_path) > 0 THEN
    @pulse_strln_info_prc_inqry = 
        EXTRACT             
            Opcode int,
            Function int, 
            PLUCode long, 
            InquiryAmt decimal, 
            PromoAmt decimal, 
            DiscAmt decimal, 
            PLUPrice decimal, 
            OrigPrice decimal, 
            Count int, 
            DecCount int, 
            Msu int?, 
            Weight int, 
            ExecuteInq int, 
            StoreCpn short, 
            VendorCpn short, 
            OtherCpn short, 
            WICItem short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_info_prc_inqry_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_info_prc_inqry ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_info_prc_inqry (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_info_prc_inqry_cnt,
	    pulse_strln_info_prc_inqry_dec_cnt,
	    pulse_strln_info_prc_inqry_disc_amt,
	    pulse_strln_info_prc_inqry_exec_inqry,
	    pulse_strln_info_prc_inqry_func,
	    pulse_strln_info_prc_inqry_amt,
	    pulse_strln_info_prc_inqry_msu,
	    pulse_strln_info_prc_inqry_orig_prc,
	    pulse_strln_info_prc_inqry_othr_cpn_ind,
	    pulse_strln_info_prc_inqry_plu_cd,
	    pulse_strln_info_prc_inqry_plu_prc,
	    pulse_strln_info_prc_inqry_promo_amt,
	    pulse_strln_info_prc_inqry_store_cpn_ind,
	    pulse_strln_info_prc_inqry_vend_cpn_ind,
	    pulse_strln_info_prc_inqry_wic_item_ind,
	    pulse_strln_info_prc_inqry_wgt,
	    pulse_strln_info_prc_inqry_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                           
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Count AS pulse_strln_info_prc_inqry_cnt, 
        DecCount AS pulse_strln_info_prc_inqry_dec_cnt, 
        (decimal)(DiscAmt / 100) AS pulse_strln_info_prc_inqry_disc_amt, 
        ExecuteInq AS pulse_strln_info_prc_inqry_exec_inqry, 
        Function AS pulse_strln_info_prc_inqry_func, 
        (decimal)(InquiryAmt / 100) AS pulse_strln_info_prc_inqry_amt, 
        (Msu ?? 0) AS pulse_strln_info_prc_inqry_msu, 
        (decimal)(OrigPrice / 100) AS pulse_strln_info_prc_inqry_orig_prc, 
        OtherCpn AS pulse_strln_info_prc_inqry_othr_cpn_ind, 
        PLUCode AS pulse_strln_info_prc_inqry_plu_cd, 
        (decimal)(PLUPrice / 100) AS pulse_strln_info_prc_inqry_plu_prc, 
        (decimal)(PromoAmt / 100) AS pulse_strln_info_prc_inqry_promo_amt, 
        StoreCpn AS pulse_strln_info_prc_inqry_store_cpn_ind, 
        VendorCpn AS pulse_strln_info_prc_inqry_vend_cpn_ind, 
        WICItem AS pulse_strln_info_prc_inqry_wic_item_ind, 
        Weight AS pulse_strln_info_prc_inqry_wgt, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_info_prc_inqry_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_info_prc_inqry;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_info_prc_inqry" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_info_prc_inqry
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_item_not_fnd 
DECLARE @pulse_strln_item_not_fnd_path string = string.Format("{0}/itemnotfound.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_item_not_fnd_path) AND FILE.LENGTH(@pulse_strln_item_not_fnd_path) > 0 THEN
    @pulse_strln_item_not_fnd = 
        EXTRACT 
            Opcode int,
            Function int, 
            PLUCode string, 
            ScannedItem short, 
            Inquiry short, 
            InfoRecord short, 
            DepSaleUnknown short, 
            ExtraCode int, 
            POSOffline short, 
            UnknownItemLogic short, 
            Unitmsrv_alive short, 
            ReceivedPLUinfo int, 
            InvalidPLUdetails short, 
            External_srv_alive short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_item_not_fnd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_item_not_fnd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_item_not_fnd (
        pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_item_not_fnd_dept_sale_unkwn_ind,
	    pulse_strln_item_not_fnd_extl_srv_alive_ind,
	    pulse_strln_item_not_fnd_addl_cd,
	    pulse_strln_item_not_fnd_func,
	    pulse_strln_item_not_fnd_info_rcd_ind,
	    pulse_strln_item_not_fnd_inqry_ind,
	    pulse_strln_item_not_fnd_invalid_plu_det_ind,
	    pulse_strln_item_not_fnd_plu_cd,
	    pulse_strln_item_not_fnd_pos_offln_ind,
	    pulse_strln_item_not_fnd_rcvd_plu_info,
	    pulse_strln_item_not_fnd_scan_item_ind,
	    pulse_strln_item_not_fnd_unit_msrv_alive_ind,
	    pulse_strln_item_not_fnd_unkwn_item_logic_ind,
	    pulse_strln_item_not_fnd_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        DepSaleUnknown AS pulse_strln_item_not_fnd_dept_sale_unkwn_ind, 
        External_srv_alive AS pulse_strln_item_not_fnd_extl_srv_alive_ind, 
        ExtraCode AS pulse_strln_item_not_fnd_addl_cd, 
        Function AS pulse_strln_item_not_fnd_func, 
        InfoRecord AS pulse_strln_item_not_fnd_info_rcd_ind, 
        Inquiry AS pulse_strln_item_not_fnd_inqry_ind, 
        InvalidPLUdetails AS pulse_strln_item_not_fnd_invalid_plu_det_ind, 
        PLUCode AS pulse_strln_item_not_fnd_plu_cd, 
        POSOffline AS pulse_strln_item_not_fnd_pos_offln_ind, 
        ReceivedPLUinfo AS pulse_strln_item_not_fnd_rcvd_plu_info, 
        ScannedItem AS pulse_strln_item_not_fnd_scan_item_ind, 
        Unitmsrv_alive AS pulse_strln_item_not_fnd_unit_msrv_alive_ind, 
        UnknownItemLogic AS pulse_strln_item_not_fnd_unkwn_item_logic_ind, 
        (short)(Src_T_E == "E" ? 0 : 1) AS pulse_strln_item_not_fnd_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_item_not_fnd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_item_not_fnd" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_item_not_fnd
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_acct_info 
DECLARE @pulse_strln_acct_info_path string = string.Format("{0}/accountinfo.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_acct_info_path) AND FILE.LENGTH(@pulse_strln_acct_info_path) > 0 THEN
    @pulse_strln_acct_info = 
        EXTRACT 
            Opcode int, 
            Function int, 
            TenderNumber int, 
            Type int, 
            Account string, 
            AuthNo string, 
            PrintAuthNo string, 
            TailDoNotProcess short, 
            TailBadRecord short, 
            TailTrainingMode short, 
            TailPosOffline short, 
            TailVoidTicket short, 
            TailReturnTicket int, 
            TailTV int,
            Unique int,
            TailStoreNumber int, 
            TailDate DateTime, 
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_acct_info_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_acct_info ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_acct_info (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_acct_info_prt_auth_nbr,
	    pulse_strln_acct_info_auth_nbr,
	    pulse_strln_acct_info_acct,
	    pulse_strln_acct_info_acct_typ,
	    pulse_strln_acct_info_tnd_nbr,
	    pulse_strln_acct_info_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        PrintAuthNo AS pulse_strln_acct_info_prt_auth_nbr, 
        AuthNo AS pulse_strln_acct_info_auth_nbr, 
        Account AS pulse_strln_acct_info_acct, 
        Type AS pulse_strln_acct_info_acct_typ, 
        TenderNumber AS pulse_strln_acct_info_tnd_nbr, 
        Function AS pulse_strln_acct_info_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_acct_info;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_acct_info" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_acct_info
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_age_info_id 
DECLARE @pulse_strln_age_info_id_path string = string.Format("{0}/ejinfoageid.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_age_info_id_path) AND FILE.LENGTH(@pulse_strln_age_info_id_path) > 0 THEN
    @pulse_strln_age_info_id = 
        EXTRACT 
            Opcode int,
            Function int, 
            age int, 
            date DateTime, 
            Bypass int, 
            Check int, 
            Accepted short, 
            BirthdayPrint int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_age_info_id_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_age_info_id ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_age_info_id (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_seq_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_oprtn_cd,
	    pulse_strln_age_info_id_accp_ind,
	    pulse_strln_age_info_id_brthdy_prt,
	    pulse_strln_age_info_id_bypass,
	    pulse_strln_age_info_id_chk,
	    pulse_strln_age_info_id_func,
	    pulse_strln_age_info_id_age,
	    pulse_strln_age_info_id_dt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        Opcode AS pulse_strln_oprtn_cd, 
        Accepted AS pulse_strln_age_info_id_accp_ind, 
        BirthdayPrint AS pulse_strln_age_info_id_brthdy_prt, 
        Bypass AS pulse_strln_age_info_id_bypass, 
        Check AS pulse_strln_age_info_id_chk, 
        Function AS pulse_strln_age_info_id_func, 
        age AS pulse_strln_age_info_id_age, 
        date AS pulse_strln_age_info_id_dt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_age_info_id;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_age_info_id" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_age_info_id
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_barcd_det 
DECLARE @pulse_strln_barcd_det_path string = string.Format("{0}/barcodedetails.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_barcd_det_path) AND FILE.LENGTH(@pulse_strln_barcd_det_path) > 0 THEN
    @pulse_strln_barcd_det = 
        EXTRACT 
            Opcode int,
            Function int, 
            BarcodeType int, 
            BarcodeLength int, 
            Opt_Scan short, 
            Opt_Key_Entered short, 
            Opt_ignore short, 
            Produced short, 
            Validated short, 
            CatalinaCoupon short, 
            CouponRejected short, 
            Cancel short, 
            Barcode string, 
            UniqueKeyIdentifier short, 
            BarcodeValue short, 
            ExtValidation short, 
            Not_Used short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_barcd_det_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_barcd_det ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_barcd_det (
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_store_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_barcd_det_barcd,
	    pulse_strln_barcd_det_barcd_length,
	    pulse_strln_barcd_det_barcd_typ,
	    pulse_strln_barcd_det_func,
	    pulse_strln_barcd_det_key_entrd_ind,
	    pulse_strln_barcd_det_scan_ind,
	    pulse_strln_barcd_det_ignore_ind,
	    pulse_strln_barcd_det_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    pulse_strln_barcd_det_oprtn_nu,
	    pulse_strln_barcd_det_nu,
	    last_load_ts,
	    pulse_strln_barcd_det_uniq,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Barcode AS pulse_strln_barcd_det_barcd, 
        BarcodeLength AS pulse_strln_barcd_det_barcd_length, 
        BarcodeType AS pulse_strln_barcd_det_barcd_typ, 
        Function AS pulse_strln_barcd_det_func, 
        Opt_Key_Entered AS pulse_strln_barcd_det_key_entrd_ind, 
        Opt_Scan AS pulse_strln_barcd_det_scan_ind, 
        Opt_ignore AS pulse_strln_barcd_det_ignore_ind, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_barcd_det_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        Convert.ToInt32(UniqueKeyIdentifier) AS pulse_strln_barcd_det_oprtn_nu, 
        Convert.ToInt32(Not_Used) AS pulse_strln_barcd_det_nu, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_barcd_det_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_barcd_det;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_barcd_det" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_barcd_det
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_brthdy 
DECLARE @pulse_strln_brthdy_path string = string.Format("{0}/birthday.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_brthdy_path) AND FILE.LENGTH(@pulse_strln_brthdy_path) > 0 THEN
    @pulse_strln_brthdy = 
        EXTRACT 
            Opcode int,
            Function int, 
            Age int, 
            Day int, 
            Month int, 
            Year int, 
            BioDOBFlg short, 
            SupervisorVerified short, 
            ScannedDOBFlag short, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_brthdy_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_brthdy ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_brthdy (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_seq_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_oprtn_cd,
	    pulse_strln_brthdy_age,
	    pulse_strln_brthdy_bio_dob_ind,
	    pulse_strln_brthdy_day,
	    pulse_strln_brthdy_func,
	    pulse_strln_brthdy_month,
	    pulse_strln_brthdy_scan_dob_ind,
	    pulse_strln_brthdy_sprvr_verified_ind,
	    pulse_strln_brthdy_yr,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        Opcode AS pulse_strln_oprtn_cd, 
        Age AS pulse_strln_brthdy_age, 
        BioDOBFlg AS pulse_strln_brthdy_bio_dob_ind, 
        Day AS pulse_strln_brthdy_day, 
        Function AS pulse_strln_brthdy_func, 
        Month AS pulse_strln_brthdy_month, 
        ScannedDOBFlag AS pulse_strln_brthdy_scan_dob_ind, 
        SupervisorVerified AS pulse_strln_brthdy_sprvr_verified_ind, 
        Year AS pulse_strln_brthdy_yr, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_brthdy;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_brthdy" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_brthdy
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_cmnty_prtnr 
DECLARE @pulse_strln_cmnty_prtnr_path string = string.Format("{0}/communitypartner.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cmnty_prtnr_path) AND FILE.LENGTH(@pulse_strln_cmnty_prtnr_path) > 0 THEN
    @pulse_strln_cmnty_prtnr = 
        EXTRACT 
            Opcode int, 
            Function int, 
            Final int, 
            Voided short, 
            MCR_Used short, 
            Scanned short, 
            CardNumber long, 
            Amount decimal, 
            Flags_NotUsed_1 int, 
            Flags_NotUsed_2 int, 
            Flags_NotUsed_3 int, 
            Flags_NotUsed_4 int, 
            TailDoNotProcess short, 
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cmnty_prtnr_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cmnty_prtnr ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cmnty_prtnr (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cmnty_prtnr_amt,
	    pulse_strln_cmnty_prtnr_crd_nbr,
	    pulse_strln_cmnty_prtnr_fnl,
	    pulse_strln_cmnty_prtnr_func,
	    pulse_strln_cmnty_prtnr_mcr_used_ind,
	    pulse_strln_cmnty_prtnr_scan_ind,
	    pulse_strln_cmnty_prtnr_void_ind,
	    pulse_strln_cmnty_prtnr_flg_nu_1,
	    pulse_strln_cmnty_prtnr_flg_nu_2,
	    pulse_strln_cmnty_prtnr_flg_nu_3,
	    pulse_strln_cmnty_prtnr_flg_nu_4,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
	    pulse_strln_cmnty_prtnr_uniq,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_cmnty_prtnr_amt, 
        CardNumber AS pulse_strln_cmnty_prtnr_crd_nbr, 
        Final AS pulse_strln_cmnty_prtnr_fnl, 
        Function AS pulse_strln_cmnty_prtnr_func, 
        MCR_Used AS pulse_strln_cmnty_prtnr_mcr_used_ind, 
        Scanned AS pulse_strln_cmnty_prtnr_scan_ind, 
        Voided AS pulse_strln_cmnty_prtnr_void_ind, 
        Flags_NotUsed_1 AS pulse_strln_cmnty_prtnr_flg_nu_1, 
        Flags_NotUsed_2 AS pulse_strln_cmnty_prtnr_flg_nu_2, 
        Flags_NotUsed_3 AS pulse_strln_cmnty_prtnr_flg_nu_3, 
        Flags_NotUsed_4 AS pulse_strln_cmnty_prtnr_flg_nu_4, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_cmnty_prtnr_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cmnty_prtnr;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cmnty_prtnr" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cmnty_prtnr
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_cust_surv 
DECLARE @pulse_strln_cust_surv_path string = string.Format("{0}/custsurvey.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cust_surv_path) AND FILE.LENGTH(@pulse_strln_cust_surv_path) > 0 THEN
    @pulse_strln_cust_surv = 
        EXTRACT 
            Opcode int, 
            Function int, 
            QuestionNbr int, 
            YesNoType int, 
            NumericType int, 
            YesNoAnswer int, 
            ZipCodeType int, 
            PhoneNbrType int, 
            NumericAnswer int, 
            ZipCode string, 
            PhoneNbr string, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cust_surv_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cust_surv ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cust_surv (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_tm,
	    pulse_strln_tckt_dt,
	    pulse_strln_seq_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cust_surv_func,
	    pulse_strln_cust_surv_numrc_typ,
	    pulse_strln_cust_surv_numrc_answr,
	    pulse_strln_cust_surv_phn_nbr,
	    pulse_strln_cust_surv_phn_nbr_typ,
	    pulse_strln_cust_surv_qustn_nbr,
	    pulse_strln_cust_surv_yes_no_ind,
	    pulse_strln_cust_surv_yes_no_typ,
	    pulse_strln_cust_surv_uniq,
	    pulse_strln_cust_surv_zip_cd,
	    pulse_strln_cust_surv_zip_cd_typ,
	    pulse_strln_cust_surv_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    pulse_strln_term_nbr,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailTime AS pulse_strln_tckt_tm, 
        TailDate AS pulse_strln_tckt_dt, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_cust_surv_func, 
        NumericType AS pulse_strln_cust_surv_numrc_typ, 
        NumericAnswer AS pulse_strln_cust_surv_numrc_answr, 
        PhoneNbr AS pulse_strln_cust_surv_phn_nbr, 
        PhoneNbrType AS pulse_strln_cust_surv_phn_nbr_typ, 
        QuestionNbr AS pulse_strln_cust_surv_qustn_nbr, 
        YesNoAnswer AS pulse_strln_cust_surv_yes_no_ind, 
        YesNoType AS pulse_strln_cust_surv_yes_no_typ, 
        Unique AS pulse_strln_cust_surv_uniq, 
        ZipCode AS pulse_strln_cust_surv_zip_cd, 
        ZipCodeType AS pulse_strln_cust_surv_zip_cd_typ, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_cust_surv_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cust_surv;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cust_surv" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cust_surv
    GROUP BY TailDate;
END;    


// pulse_strln_info_rsn_cd 
DECLARE @pulse_strln_info_rsn_cd_path string = string.Format("{0}/inforeasoncode.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_info_rsn_cd_path) AND FILE.LENGTH(@pulse_strln_info_rsn_cd_path) > 0 THEN
    @pulse_strln_info_rsn_cd = 
        EXTRACT 
            Opcode int,
            Function int, 
            NumberOfLine int, 
            Comment string, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_info_rsn_cd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_info_rsn_cd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_info_rsn_cd (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_info_rsn_cd_func,
	    pulse_strln_info_rsn_cd_nbr_ln,
	    pulse_strln_info_rsn_cd_cmt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm,
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_info_rsn_cd_func, 
        NumberOfLine AS pulse_strln_info_rsn_cd_nbr_ln, 
        Comment AS pulse_strln_info_rsn_cd_cmt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_info_rsn_cd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_info_rsn_cd" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_info_rsn_cd
    GROUP BY TailDate;
END;    


// pulse_strln_saved_tckt 
DECLARE @pulse_strln_saved_tckt_path string = string.Format("{0}/savedticket.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_saved_tckt_path) AND FILE.LENGTH(@pulse_strln_saved_tckt_path) > 0 THEN
    @pulse_strln_saved_tckt = 
        EXTRACT 
            Opcode int, 
            Function int, 
            TicketNo int, 
            SaveAtSaleMode short, 
            SaveAtTenderMode short, 
            SaveAtTrainingMode short, 
            SavedForPayment short, 
            SavedForSplitPayment short, 
            PackTransaction int, 
            TaxVoucherRequested int, 
            SaveAtStockCount short, 
            TicketAmt decimal, 
            MediaTotal int, 
            TicketItems int, 
            SupervisorNo int, 
            InvNo string, 
            InvDate DateTime, 
            RecallPOSNo int, 
            RecallTicketNo int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_saved_tckt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_saved_tckt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_saved_tckt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_saved_tckt_func,
	    pulse_strln_saved_tckt_invc_dt,
	    pulse_strln_saved_tckt_invc_nbr,
	    pulse_strln_saved_tckt_media_tot,
	    pulse_strln_saved_tckt_pk_trans,
	    pulse_strln_saved_tckt_recall_pos_nbr,
	    pulse_strln_saved_tckt_recall_tckt_nbr,
	    pulse_strln_saved_tckt_save_sale_mode_ind,
	    pulse_strln_saved_tckt_save_stk_cnt_ind,
	    pulse_strln_saved_tckt_save_tnd_mode_ind,
	    pulse_strln_saved_tckt_save_train_mode_ind,
	    pulse_strln_saved_tckt_saved_pmt_ind,
	    pulse_strln_saved_tckt_saved_splt_pmt_ind,
	    pulse_strln_saved_tckt_sprvr_nbr,
	    pulse_strln_saved_tckt_tax_vchr_requested,
	    pulse_strln_saved_tckt_amt,
	    pulse_strln_saved_tckt_items,
	    pulse_strln_saved_tckt_nbr,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm,
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_saved_tckt_func, 
        InvDate AS pulse_strln_saved_tckt_invc_dt, 
        InvNo AS pulse_strln_saved_tckt_invc_nbr, 
        MediaTotal AS pulse_strln_saved_tckt_media_tot, 
        PackTransaction AS pulse_strln_saved_tckt_pk_trans, 
        RecallPOSNo AS pulse_strln_saved_tckt_recall_pos_nbr, 
        RecallTicketNo AS pulse_strln_saved_tckt_recall_tckt_nbr, 
        SaveAtSaleMode AS pulse_strln_saved_tckt_save_sale_mode_ind, 
        SaveAtStockCount AS pulse_strln_saved_tckt_save_stk_cnt_ind, 
        SaveAtTenderMode AS pulse_strln_saved_tckt_save_tnd_mode_ind, 
        SaveAtTrainingMode AS pulse_strln_saved_tckt_save_train_mode_ind, 
        SavedForPayment AS pulse_strln_saved_tckt_saved_pmt_ind, 
        SavedForSplitPayment AS pulse_strln_saved_tckt_saved_splt_pmt_ind, 
        SupervisorNo AS pulse_strln_saved_tckt_sprvr_nbr, 
        TaxVoucherRequested AS pulse_strln_saved_tckt_tax_vchr_requested, 
        (decimal)(TicketAmt / 100) AS pulse_strln_saved_tckt_amt, 
        TicketItems AS pulse_strln_saved_tckt_items, 
        TicketNo AS pulse_strln_saved_tckt_nbr, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_saved_tckt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_saved_tckt" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_saved_tckt
    GROUP BY TailDate;
END;


// pulse_strln_rcld_tckt 
DECLARE @pulse_strln_rcld_tckt_path string = string.Format("{0}/recalledticket.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_rcld_tckt_path) AND FILE.LENGTH(@pulse_strln_rcld_tckt_path) > 0 THEN
    @pulse_strln_rcld_tckt = 
        EXTRACT 
            Opcode int, 
            Function int, 
            RecallTicketNo int, 
            RecallPOSNo int, 
            RecallCashier int, 
            Start int, 
            OfflineMode int, 
            ManualEntryValues int, 
            InvoiceNotFound int, 
            RecallSavedBakery int, 
            RecallDriveOff int, 
            QuickRecallFlag short, 
            ExtendedRecall int, 
            Branch int, 
            CheckoutBank int, 
            InvoiceAmt decimal, 
            InvoiceNo string, 
            InvoiceDate DateTime, 
            SequenceNo int, 
            Time string, 
            SavedTillType int, 
            SavedTillProfile int, 
            RecallMode int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_rcld_tckt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_rcld_tckt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_rcld_tckt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_rcld_tckt_branch,
	    pulse_strln_rcld_tckt_chkout_bank,
	    pulse_strln_rcld_tckt_ext_recall,
	    pulse_strln_rcld_tckt_func,
	    pulse_strln_rcld_tckt_invc_amt,
	    pulse_strln_rcld_tckt_invc_dt,
	    pulse_strln_rcld_tckt_invc_nbr,
	    pulse_strln_rcld_tckt_invc_not_fnd,
	    pulse_strln_rcld_tckt_manl_entry_values,
	    pulse_strln_rcld_tckt_offln_mode,
	    pulse_strln_rcld_tckt_quick_recall_ind,
	    pulse_strln_rcld_tckt_recall_cshr,
	    pulse_strln_rcld_tckt_recall_drive_off,
	    pulse_strln_rcld_tckt_recall_mode,
	    pulse_strln_rcld_tckt_recall_pos_nbr,
	    pulse_strln_rcld_tckt_recall_saved_bakery,
	    pulse_strln_rcld_tckt_recall_tckt_nbr,
	    pulse_strln_rcld_tckt_saved_till_prof,
	    pulse_strln_rcld_tckt_saved_till_typ,
	    pulse_strln_rcld_tckt_seq_nbr,
	    pulse_strln_rcld_tckt_strt,
	    pulse_strln_rcld_tckt_tm,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Branch AS pulse_strln_rcld_tckt_branch, 
        CheckoutBank AS pulse_strln_rcld_tckt_chkout_bank, 
        ExtendedRecall AS pulse_strln_rcld_tckt_ext_recall, 
        Function AS pulse_strln_rcld_tckt_func, 
        (decimal)(InvoiceAmt / 100) AS pulse_strln_rcld_tckt_invc_amt, 
        InvoiceDate AS pulse_strln_rcld_tckt_invc_dt, 
        InvoiceNo AS pulse_strln_rcld_tckt_invc_nbr, 
        InvoiceNotFound AS pulse_strln_rcld_tckt_invc_not_fnd, 
        ManualEntryValues AS pulse_strln_rcld_tckt_manl_entry_values, 
        OfflineMode AS pulse_strln_rcld_tckt_offln_mode, 
        QuickRecallFlag AS pulse_strln_rcld_tckt_quick_recall_ind, 
        RecallCashier AS pulse_strln_rcld_tckt_recall_cshr, 
        RecallDriveOff AS pulse_strln_rcld_tckt_recall_drive_off, 
        RecallMode AS pulse_strln_rcld_tckt_recall_mode, 
        RecallPOSNo AS pulse_strln_rcld_tckt_recall_pos_nbr, 
        RecallSavedBakery AS pulse_strln_rcld_tckt_recall_saved_bakery, 
        RecallTicketNo AS pulse_strln_rcld_tckt_recall_tckt_nbr, 
        SavedTillProfile AS pulse_strln_rcld_tckt_saved_till_prof, 
        SavedTillType AS pulse_strln_rcld_tckt_saved_till_typ, 
        SequenceNo AS pulse_strln_rcld_tckt_seq_nbr, 
        Start AS pulse_strln_rcld_tckt_strt, 
        Time AS pulse_strln_rcld_tckt_tm, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_rcld_tckt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_rcld_tckt" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_rcld_tckt
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_cash_office_depst 
DECLARE @pulse_strln_cash_office_depst_path string = string.Format("{0}/cashofficedeposit.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cash_office_depst_path) AND FILE.LENGTH(@pulse_strln_cash_office_depst_path) > 0 THEN
    @pulse_strln_cash_office_depst = 
        EXTRACT 
            Opcode int, 
            Function int, 
            Amount decimal, 
            Count int, 
            Tender int, 
            Correction short, 
            UsePrevWorkDay short, 
            Reprocess short, 
            Header int, 
            Info short, 
            Key int, 
            PcNumber int, 
            BankId int, 
            Reference string, 
            SafeNumber int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cash_office_depst_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cash_office_depst ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cash_office_depst (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_seq_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cash_office_depst_func,
	    pulse_strln_cash_office_depst_amt,
	    pulse_strln_cash_office_depst_corrtn_ind,
	    pulse_strln_cash_office_depst_cnt,
	    pulse_strln_cash_office_depst_tnd,
	    pulse_strln_cash_office_depst_reproc_ind,
	    pulse_strln_cash_office_depst_hdr,
	    pulse_strln_cash_office_depst_info_ind,
	    pulse_strln_cash_office_depst_key,
	    pulse_strln_cash_office_depst_pc_nbr,
	    pulse_strln_cash_office_depst_bank_id,
	    pulse_strln_cash_office_depst_ref,
	    pulse_strln_cash_office_depst_safe_nbr,
	    pulse_strln_cash_office_depst_use_prev_work_day_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_cash_office_depst_func, 
        (decimal)(Amount / 100) AS pulse_strln_cash_office_depst_amt, 
        Correction AS pulse_strln_cash_office_depst_corrtn_ind, 
        Count AS pulse_strln_cash_office_depst_cnt, 
        Tender AS pulse_strln_cash_office_depst_tnd, 
        Reprocess AS pulse_strln_cash_office_depst_reproc_ind, 
        Header AS pulse_strln_cash_office_depst_hdr, 
        Info AS pulse_strln_cash_office_depst_info_ind, 
        Key AS pulse_strln_cash_office_depst_key, 
        PcNumber AS pulse_strln_cash_office_depst_pc_nbr, 
        BankId AS pulse_strln_cash_office_depst_bank_id, 
        Reference AS pulse_strln_cash_office_depst_ref, 
        SafeNumber AS pulse_strln_cash_office_depst_safe_nbr, 
        UsePrevWorkDay AS pulse_strln_cash_office_depst_use_prev_work_day_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cash_office_depst;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cash_office_depst" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cash_office_depst
    GROUP BY TailDate;
END;    


// pulse_strln_cash_office_rcpt 
DECLARE @pulse_strln_cash_office_rcpt_path string = string.Format("{0}/cashofficereceipt.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cash_office_rcpt_path) AND FILE.LENGTH(@pulse_strln_cash_office_rcpt_path) > 0 THEN
    @pulse_strln_cash_office_rcpt = 
        EXTRACT 
            Opcode int, 
            Function int, 
            Amount decimal, 
            Count int, 
            Tender int, 
            Correction int, 
            UsePrevWorkDay short, 
            Reprocess short, 
            Header int, 
            Info short, 
            Key int, 
            PcNumber int, 
            BankId int, 
            Reference string, 
            SafeNumber int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cash_office_rcpt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cash_office_rcpt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cash_office_rcpt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cash_office_rcpt_func,
	    pulse_strln_cash_office_rcpt_amt,
	    pulse_strln_cash_office_rcpt_corrtn,
	    pulse_strln_cash_office_rcpt_cnt,
	    pulse_strln_cash_office_rcpt_tnd,
	    pulse_strln_cash_office_rcpt_reproc_ind,
	    pulse_strln_cash_office_rcpt_hdr,
	    pulse_strln_cash_office_rcpt_info_ind,
	    pulse_strln_cash_office_rcpt_key,
	    pulse_strln_cash_office_rcpt_pc_nbr,
	    pulse_strln_cash_office_rcpt_bank_id,
	    pulse_strln_cash_office_rcpt_ref,
	    pulse_strln_cash_office_rcpt_safe_nbr,
	    pulse_strln_cash_office_rcpt_use_prev_work_day_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_cash_office_rcpt_func, 
        (decimal)(Amount / 100) AS pulse_strln_cash_office_rcpt_amt, 
        Correction AS pulse_strln_cash_office_rcpt_corrtn, 
        Count AS pulse_strln_cash_office_rcpt_cnt, 
        Tender AS pulse_strln_cash_office_rcpt_tnd, 
        Reprocess AS pulse_strln_cash_office_rcpt_reproc_ind, 
        Header AS pulse_strln_cash_office_rcpt_hdr, 
        Info AS pulse_strln_cash_office_rcpt_info_ind, 
        Key AS pulse_strln_cash_office_rcpt_key, 
        PcNumber AS pulse_strln_cash_office_rcpt_pc_nbr, 
        BankId AS pulse_strln_cash_office_rcpt_bank_id, 
        Reference AS pulse_strln_cash_office_rcpt_ref, 
        SafeNumber AS pulse_strln_cash_office_rcpt_safe_nbr, 
        UsePrevWorkDay AS pulse_strln_cash_office_rcpt_use_prev_work_day_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cash_office_rcpt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cash_office_rcpt" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cash_office_rcpt
    GROUP BY TailDate;
END;    


// pulse_strln_cash_pkup 
DECLARE @pulse_strln_cash_pkup_path string = string.Format("{0}/cashpickup.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cash_pkup_path) AND FILE.LENGTH(@pulse_strln_cash_pkup_path) > 0 THEN
    @pulse_strln_cash_pkup = 
        EXTRACT             
            Opcode int, 
            Function int, 
            Amount decimal, 
            TenderNumber int, 
            Count int, 
            Total int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_cash_pkup_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cash_pkup ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cash_pkup (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cash_pkup_amt,
	    pulse_strln_cash_pkup_cnt,
	    pulse_strln_cash_pkup_func,
	    pulse_strln_cash_pkup_tnd_nbr,
	    pulse_strln_cash_pkup_tot,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_cash_pkup_amt, 
        Count AS pulse_strln_cash_pkup_cnt, 
        Function AS pulse_strln_cash_pkup_func, 
        TenderNumber AS pulse_strln_cash_pkup_tnd_nbr, 
        Total AS pulse_strln_cash_pkup_tot, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cash_pkup;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cash_pkup" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cash_pkup
    GROUP BY TailDate;
END;    


// pulse_strln_loan 
DECLARE @pulse_strln_loan_path string = string.Format("{0}/loan.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_loan_path) AND FILE.LENGTH(@pulse_strln_loan_path) > 0 THEN
    @pulse_strln_loan = 
        EXTRACT 
            Opcode int, 
            Function int, 
            Amount decimal, 
            Count int, 
            TenderNumber int, 
            Total int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_loan_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_loan ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_loan (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_loan_func,
	    pulse_strln_loan_amt,
	    pulse_strln_loan_cnt,
	    pulse_strln_loan_tnd_nbr,
	    pulse_strln_loan_tot,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_loan_func, 
        (decimal)(Amount / 100) AS pulse_strln_loan_amt, 
        Count AS pulse_strln_loan_cnt, 
        TenderNumber AS pulse_strln_loan_tnd_nbr, 
        Total AS pulse_strln_loan_tot, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_loan;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_loan" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_loan
    GROUP BY TailDate;
END;    


// pulse_strln_payout_rcpt 
DECLARE @pulse_strln_payout_rcpt_path string = string.Format("{0}/payoutreceipt.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_payout_rcpt_path) AND FILE.LENGTH(@pulse_strln_payout_rcpt_path) > 0 THEN
    @pulse_strln_payout_rcpt = 
        EXTRACT 
            Opcode int, 
            ExpenseCode int, 
            ExpenseReference string, 
            Amount decimal, 
            TenderNumber int, 
            POSAccountability int, 
            Reprocess short, 
            AmountInteger64 short, 
            Enhanced short, 
            AutoTrans short, 
            TenderByUnit int, 
            PaidOut short, 
            Receipt short, 
            EnhancedROACode int, 
            VATAmount decimal, 
            ROACode string, 
            FrontOfficeUserID int, 
            SafeNumber int, 
            Quantity int, 
            ROACorrection int, 
            amount2 decimal, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_payout_rcpt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_payout_rcpt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_payout_rcpt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_payout_rcpt_exp_cd,
	    pulse_strln_payout_rcpt_exp_ref,
	    pulse_strln_payout_rcpt_amt,
	    pulse_strln_payout_rcpt_tnd_nbr,
	    pulse_strln_payout_rcpt_pos_acctbty,
	    pulse_strln_payout_rcpt_reproc_ind,
	    pulse_strln_payout_rcpt_long_amt_integer_ind,
	    pulse_strln_payout_rcpt_enhcd_ind,
	    pulse_strln_payout_rcpt_auto_trans_ind,
	    pulse_strln_payout_rcpt_tnd_by_unit,
	    pulse_strln_payout_rcpt_paidout_ind,
	    pulse_strln_payout_rcpt_rcpt_ind,
	    pulse_strln_payout_rcpt_enhcd_rcvd_acct_cd,
	    pulse_strln_payout_rcpt_val_added_tax_amt,
	    pulse_strln_payout_rcpt_rcvd_acct_cd,
	    pulse_strln_payout_rcpt_frnt_office_user_id,
	    pulse_strln_payout_rcpt_safe_nbr,
	    pulse_strln_payout_rcpt_qty,
	    pulse_strln_payout_rcpt_rcvd_on_acct_corrtn,
	    pulse_strln_payout_rcpt_another_amt,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        ExpenseCode AS pulse_strln_payout_rcpt_exp_cd, 
        ExpenseReference AS pulse_strln_payout_rcpt_exp_ref, 
        (decimal)(Amount / 100) AS pulse_strln_payout_rcpt_amt, 
        TenderNumber AS pulse_strln_payout_rcpt_tnd_nbr, 
        POSAccountability AS pulse_strln_payout_rcpt_pos_acctbty, 
        Reprocess AS pulse_strln_payout_rcpt_reproc_ind, 
        AmountInteger64 AS pulse_strln_payout_rcpt_long_amt_integer_ind, 
        Enhanced AS pulse_strln_payout_rcpt_enhcd_ind, 
        AutoTrans AS pulse_strln_payout_rcpt_auto_trans_ind, 
        TenderByUnit AS pulse_strln_payout_rcpt_tnd_by_unit, 
        PaidOut AS pulse_strln_payout_rcpt_paidout_ind, 
        Receipt AS pulse_strln_payout_rcpt_rcpt_ind, 
        EnhancedROACode AS pulse_strln_payout_rcpt_enhcd_rcvd_acct_cd, 
        (decimal)(VATAmount / 100) AS pulse_strln_payout_rcpt_val_added_tax_amt, 
        ROACode AS pulse_strln_payout_rcpt_rcvd_acct_cd, 
        FrontOfficeUserID AS pulse_strln_payout_rcpt_frnt_office_user_id, 
        SafeNumber AS pulse_strln_payout_rcpt_safe_nbr, 
        Quantity AS pulse_strln_payout_rcpt_qty, 
        ROACorrection AS pulse_strln_payout_rcpt_rcvd_on_acct_corrtn, 
        (decimal)(amount2 / 100) AS pulse_strln_payout_rcpt_another_amt, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_payout_rcpt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_payout_rcpt" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_payout_rcpt
    GROUP BY TailDate;
END;    


// pulse_strln_cntl_chk 
DECLARE @pulse_strln_cntl_chk_path string = string.Format("{0}/controlcheck.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cntl_chk_path) AND FILE.LENGTH(@pulse_strln_cntl_chk_path) > 0 THEN
    @pulse_strln_cntl_chk = 
        EXTRACT 
            Opcode int,
            Function int,
            CCNumber int,
            Accepted short, 
            AskYesNo short, 
            MgrKey int,
            StopActivity int,
            SupervisorKey int,
            WarningOnly short, 
            ValidRecord short, 
            DelayedAuth short, 
            ValueAtText int,
            Privilege int,
            CashierNo int,
            TemplateNo int, 
            POSStatus int,
            TlogRptLevel int,
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cntl_chk_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cntl_chk ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cntl_chk (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cntl_chk_accp_ind,
	    pulse_strln_cntl_chk_ask_yes_no_ind,
	    pulse_strln_cntl_chk_cc_nbr,
	    pulse_strln_cntl_chk_cshr_nbr,
	    pulse_strln_cntl_chk_dlay_auth_ind,
	    pulse_strln_cntl_chk_func,
	    pulse_strln_cntl_chk_mgr_key,
	    pulse_strln_cntl_chk_pos_stat,
	    pulse_strln_cntl_chk_prvlg,
	    pulse_strln_cntl_chk_stop_actvy,
	    pulse_strln_cntl_chk_sprvr_key,
	    pulse_strln_cntl_chk_tmplt_nbr,
	    pulse_strln_cntl_chk_tlog_rpt_lvl,
	    pulse_strln_cntl_chk_valid_rcd_ind,
	    pulse_strln_cntl_chk_val_txt,
	    pulse_strln_cntl_chk_warn_only_ind,
	    pulse_strln_cntl_chk_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Accepted AS pulse_strln_cntl_chk_accp_ind, 
        AskYesNo AS pulse_strln_cntl_chk_ask_yes_no_ind, 
        CCNumber AS pulse_strln_cntl_chk_cc_nbr, 
        CashierNo AS pulse_strln_cntl_chk_cshr_nbr, 
        DelayedAuth AS pulse_strln_cntl_chk_dlay_auth_ind, 
        Function AS pulse_strln_cntl_chk_func, 
        MgrKey AS pulse_strln_cntl_chk_mgr_key, 
        POSStatus AS pulse_strln_cntl_chk_pos_stat, 
        Privilege AS pulse_strln_cntl_chk_prvlg, 
        StopActivity AS pulse_strln_cntl_chk_stop_actvy, 
        SupervisorKey AS pulse_strln_cntl_chk_sprvr_key, 
        TemplateNo AS pulse_strln_cntl_chk_tmplt_nbr, 
        TlogRptLevel AS pulse_strln_cntl_chk_tlog_rpt_lvl, 
        ValidRecord AS pulse_strln_cntl_chk_valid_rcd_ind, 
        ValueAtText AS pulse_strln_cntl_chk_val_txt, 
        WarningOnly AS pulse_strln_cntl_chk_warn_only_ind, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_cntl_chk_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cntl_chk;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cntl_chk" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cntl_chk
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_plu_sale 
DECLARE @pulse_strln_plu_sale_path string = string.Format("{0}/plusale.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_plu_sale_path) AND FILE.LENGTH(@pulse_strln_plu_sale_path) > 0 THEN
    @pulse_strln_plu_sale = 
        EXTRACT 
            Opcode int, 
            PluNo long, 
            Subtract short, 
            Cancel short, 
            Negative short, 
            opt_was_cancelled short, 
            opt_supplier_promotion short, 
            opt_staff_discountable short, 
            opt_accept_price_override short, 
            PriceOverride short, 
            ManualPrice short, 
            WeightFromScale short, 
            QtyIsWeight short, 
            ChainedPrevItem short, 
            NonMerchandise short, 
            StoreCoupon short, 
            VendorCoupon short, 
            ItemDiscountable short, 
            ScannedItem short, 
            Non_RXItem short?, 
            opt_WIC_CVV short, 
            opt_manual_tare_weight short, 
            opt_freq_shop_disc short, 
            FSPayment short, 
            DepartmentNo int, 
            MultipleSellUnits int, 
            ReturnType int, 
            TaxPointer int, 
            Qty int, 
            Price decimal, 
            Amount decimal, 
            nt_amount decimal, 
            opt_rss_item_sale short, 
            BottleDeposit short, 
            BottleRefund short, 
            opt_Rx short, 
            opt_qty_case short, 
            TailDoNotProcess short, 
            TailBadRecord short, 
            TailTrainingMode short, 
            TailPosOffline short, 
            TailVoidTicket int, 
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int, 
            TailDate DateTime, 
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_plu_sale_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_plu_sale ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_plu_sale (
        pulse_strln_store_nbr, 
        pulse_strln_cshr_nbr, 
        pulse_strln_rgstr_nbr, 
        pulse_strln_tckt_nbr, 
        pulse_strln_tckt_dt, 
        pulse_strln_tckt_tm, 
        pulse_strln_seq_nbr, 
        pulse_strln_oprtn_cd, 
        pulse_strln_plu_sale_amt, 
        pulse_strln_plu_sale_bottle_depst_ind, 
        pulse_strln_plu_sale_bottle_rfnd_ind, 
        pulse_strln_plu_sale_cancel_ind, 
        pulse_strln_plu_sale_chain_prev_item_ind, 
        pulse_strln_plu_sale_dept_nbr, 
        pulse_strln_plu_sale_fd_stmp_pmt_ind, 
        pulse_strln_plu_sale_item_disc_ind, 
        pulse_strln_plu_sale_manl_prc_ind, 
        pulse_strln_plu_sale_mult_sell_units, 
        pulse_strln_plu_sale_neg_ind, 
        pulse_strln_plu_sale_non_rx_item_ind, 
        pulse_strln_plu_sale_non_mdse_ind, 
        pulse_strln_plu_sale_net_amt, 
        pulse_strln_plu_sale_plu_nbr, 
        pulse_strln_plu_sale_prc, 
        pulse_strln_plu_sale_prc_ovrd_ind, 
        pulse_strln_plu_sale_qty, 
        pulse_strln_plu_sale_qty_wgt_ind,
        pulse_strln_plu_sale_rtn_typ, 
        pulse_strln_plu_sale_scan_item_ind, 
        pulse_strln_plu_sale_store_cpn_ind, 
        pulse_strln_plu_sale_subtrct_ind, 
        pulse_strln_plu_sale_tax_pntr, 
        pulse_strln_plu_sale_vend_cpn_ind, 
        pulse_strln_plu_sale_wgt_from_scale_ind, 
        pulse_strln_plu_sale_accept_prc_ovrd_ind, 
        pulse_strln_plu_sale_freq_shop_disc_ind, 
        pulse_strln_plu_sale_manl_tare_wgt_ind, 
        pulse_strln_plu_sale_qty_case_ind, 
        pulse_strln_plu_sale_rss_item_sale_ind, 
        pulse_strln_plu_sale_phar_ind, 
        pulse_strln_plu_sale_staff_disc_ind, 
        pulse_strln_plu_sale_supplr_promo_ind, 
        pulse_strln_plu_sale_wic_crd_vrfctn_val_ind, 
        pulse_strln_bad_rcd_ind, 
        pulse_strln_dt, 
        pulse_strln_tm, 
        pulse_strln_do_not_proc_ind, 
        pulse_strln_term_nbr, 
        pulse_strln_pos_offln_ind, 
        pulse_strln_rtn_tckt, 
        pulse_strln_tv, 
        pulse_strln_train_mode_ind, 
        pulse_strln_void_tckt_ind, 
        last_load_ts, 
        pulse_strln_plu_sale_uniq, 
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        (decimal)(Amount / 100) AS pulse_strln_plu_sale_amt, 
        BottleDeposit AS pulse_strln_plu_sale_bottle_depst_ind, 
        BottleRefund AS pulse_strln_plu_sale_bottle_rfnd_ind, 
        Cancel AS pulse_strln_plu_sale_cancel_ind, 
        ChainedPrevItem AS pulse_strln_plu_sale_chain_prev_item_ind, 
        DepartmentNo AS pulse_strln_plu_sale_dept_nbr, 
        FSPayment AS pulse_strln_plu_sale_fd_stmp_pmt_ind, 
        ItemDiscountable AS pulse_strln_plu_sale_item_disc_ind, 
        ManualPrice AS pulse_strln_plu_sale_manl_prc_ind, 
        MultipleSellUnits AS pulse_strln_plu_sale_mult_sell_units, 
        Negative AS pulse_strln_plu_sale_neg_ind, 
        (Non_RXItem ?? 0) AS pulse_strln_plu_sale_non_rx_item_ind, 
        NonMerchandise AS pulse_strln_plu_sale_non_mdse_ind, 
        (decimal)(nt_amount / 100) AS pulse_strln_plu_sale_net_amt, 
        PluNo AS pulse_strln_plu_sale_plu_nbr, 
        (decimal)(Price / 100) AS pulse_strln_plu_sale_prc, 
        PriceOverride AS pulse_strln_plu_sale_prc_ovrd_ind, 
        Qty AS pulse_strln_plu_sale_qty, 
        QtyIsWeight AS pulse_strln_plu_sale_qty_wgt_ind,
        ReturnType AS pulse_strln_plu_sale_rtn_typ, 
        ScannedItem AS pulse_strln_plu_sale_scan_item_ind, 
        StoreCoupon AS pulse_strln_plu_sale_store_cpn_ind, 
        Subtract AS pulse_strln_plu_sale_subtrct_ind, 
        TaxPointer AS pulse_strln_plu_sale_tax_pntr, 
        VendorCoupon AS pulse_strln_plu_sale_vend_cpn_ind, 
        WeightFromScale AS pulse_strln_plu_sale_wgt_from_scale_ind, 
        opt_accept_price_override AS pulse_strln_plu_sale_accept_prc_ovrd_ind, 
        opt_freq_shop_disc AS pulse_strln_plu_sale_freq_shop_disc_ind, 
        opt_manual_tare_weight AS pulse_strln_plu_sale_manl_tare_wgt_ind, 
        opt_qty_case AS pulse_strln_plu_sale_qty_case_ind, 
        opt_rss_item_sale AS pulse_strln_plu_sale_rss_item_sale_ind, 
        opt_Rx AS pulse_strln_plu_sale_phar_ind, 
        opt_staff_discountable AS pulse_strln_plu_sale_staff_disc_ind, 
        opt_supplier_promotion AS pulse_strln_plu_sale_supplr_promo_ind, 
        opt_WIC_CVV AS pulse_strln_plu_sale_wic_crd_vrfctn_val_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_plu_sale_uniq,     
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_plu_sale;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_plu_sale" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_plu_sale
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_cshr_prfmc_stats 
DECLARE @pulse_strln_cshr_prfmc_stats_path string = string.Format("{0}/cashierperformancestatistics.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cshr_prfmc_stats_path) AND FILE.LENGTH(@pulse_strln_cshr_prfmc_stats_path) > 0 THEN
    @pulse_strln_cshr_prfmc_stats = 
        EXTRACT 
            Opcode int,
            POSMode int,
            TotalItemRegistrationTime int,
            TotalIdleTime int,
            TotalSecureTime int,
            TotalTenderTime int,
            TotalWaitTime int,
            TotalInquiryTime int,
            TotalOtherTime int,
            QuarterHour int,
            SignOff int,
            ExpressOrder int,
            DeltaItemRegistrationTime int,
            DeltaIdleTime int,
            DeltaSecureTime int,
            DeltaTenderTime int,
            DeltaWaitTime int,
            DeltaInquiryTime int,
            DeltaOtherTime int,
            TotalSignOnTime int,
            DeltaSignOnTime int,
            Maintenance1 int,
            Maintenance2 int,
            LocalMaintenanceQ int,
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_cshr_prfmc_stats_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_cshr_prfmc_stats ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_cshr_prfmc_stats (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_cshr_prfmc_stats_pos_mode,
	    pulse_strln_cshr_prfmc_stats_tot_wait_tm,
	    pulse_strln_cshr_prfmc_stats_tot_tnd_tm,
	    pulse_strln_cshr_prfmc_stats_tot_signon_tm,
	    pulse_strln_cshr_prfmc_stats_tot_secure_tm,
	    pulse_strln_cshr_prfmc_stats_tot_othr_tm,
	    pulse_strln_cshr_prfmc_stats_tot_item_rgstrtn_tm,
	    pulse_strln_cshr_prfmc_stats_tot_inqry_tm,
	    pulse_strln_cshr_prfmc_stats_tot_idle_tm,
	    pulse_strln_cshr_prfmc_stats_signoff,
	    pulse_strln_cshr_prfmc_stats_qtr_hour,
	    pulse_strln_cshr_prfmc_stats_maint_2,
	    pulse_strln_cshr_prfmc_stats_maint_1,
	    pulse_strln_cshr_prfmc_stats_pend_maint_rcd,
	    pulse_strln_cshr_prfmc_stats_exprs_ord,
	    pulse_strln_cshr_prfmc_stats_delta_wait_tm,
	    pulse_strln_cshr_prfmc_stats_delta_tnd_tm,
	    pulse_strln_cshr_prfmc_stats_delta_signon_tm,
	    pulse_strln_cshr_prfmc_stats_delta_secure_tm,
	    pulse_strln_cshr_prfmc_stats_delta_othr_tm,
	    pulse_strln_cshr_prfmc_stats_delta_item_rgstrtn_tm,
	    pulse_strln_cshr_prfmc_stats_delta_inqry_tm,
	    pulse_strln_cshr_prfmc_stats_delta_idle_tm,
	    pulse_strln_cshr_prfmc_stats_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
	    pulse_strln_cshr_prfmc_stats_uniq,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm,         
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        POSMode AS pulse_strln_cshr_prfmc_stats_pos_mode, 
        TotalWaitTime AS pulse_strln_cshr_prfmc_stats_tot_wait_tm, 
        TotalTenderTime AS pulse_strln_cshr_prfmc_stats_tot_tnd_tm, 
        TotalSignOnTime AS pulse_strln_cshr_prfmc_stats_tot_signon_tm, 
        TotalSecureTime AS pulse_strln_cshr_prfmc_stats_tot_secure_tm, 
        TotalOtherTime AS pulse_strln_cshr_prfmc_stats_tot_othr_tm, 
        TotalItemRegistrationTime AS pulse_strln_cshr_prfmc_stats_tot_item_rgstrtn_tm, 
        TotalInquiryTime AS pulse_strln_cshr_prfmc_stats_tot_inqry_tm, 
        TotalIdleTime AS pulse_strln_cshr_prfmc_stats_tot_idle_tm, 
        SignOff AS pulse_strln_cshr_prfmc_stats_signoff, 
        QuarterHour AS pulse_strln_cshr_prfmc_stats_qtr_hour, 
        Maintenance2 AS pulse_strln_cshr_prfmc_stats_maint_2, 
        Maintenance1 AS pulse_strln_cshr_prfmc_stats_maint_1, 
        LocalMaintenanceQ AS pulse_strln_cshr_prfmc_stats_pend_maint_rcd, 
        ExpressOrder AS pulse_strln_cshr_prfmc_stats_exprs_ord, 
        DeltaWaitTime AS pulse_strln_cshr_prfmc_stats_delta_wait_tm, 
        DeltaTenderTime AS pulse_strln_cshr_prfmc_stats_delta_tnd_tm, 
        DeltaSignOnTime AS pulse_strln_cshr_prfmc_stats_delta_signon_tm, 
        DeltaSecureTime AS pulse_strln_cshr_prfmc_stats_delta_secure_tm, 
        DeltaOtherTime AS pulse_strln_cshr_prfmc_stats_delta_othr_tm, 
        DeltaItemRegistrationTime AS pulse_strln_cshr_prfmc_stats_delta_item_rgstrtn_tm, 
        DeltaInquiryTime AS pulse_strln_cshr_prfmc_stats_delta_inqry_tm, 
        DeltaIdleTime AS pulse_strln_cshr_prfmc_stats_delta_idle_tm, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_cshr_prfmc_stats_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Unique AS pulse_strln_cshr_prfmc_stats_uniq, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_cshr_prfmc_stats;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_cshr_prfmc_stats" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_cshr_prfmc_stats
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_loc 
DECLARE @pulse_strln_loc_path string = string.Format("{0}/location.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_loc_path) AND FILE.LENGTH(@pulse_strln_loc_path) > 0 THEN
    @pulse_strln_loc = 
        EXTRACT             
            Opcode int,
            Function int,
            TenderCorrection int,
            FuelMergeRecall int,
            Invoice int,
            Frgn_cust short, 
            Express_pos int,
            Rdh_sale int,
            DepositPrevTicket short, 
            Rdh_return int,
            StoreID int,
            POSTermType int,
            CheckoutBankNo int,
            EFTLocation int,
            POSTermProfile int,
            DocType int,
            POSTermNo int,
            CashierNo int,
            wholesale_trs_type int,
            InvoiceNo string, 
            InvoiceDate DateTime, 
            InvoiceTime string, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_loc_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_loc ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_loc (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_loc_cshr_nbr,
	    pulse_strln_loc_chkout_bank_nbr,
	    pulse_strln_loc_depst_prev_tckt_ind,
	    pulse_strln_loc_doc_typ,
	    pulse_strln_loc_eft_loc,
	    pulse_strln_loc_exprs_pos,
	    pulse_strln_loc_frgn_cust_ind,
	    pulse_strln_loc_fuel_merger_call,
	    pulse_strln_loc_func,
	    pulse_strln_loc_invc,
	    pulse_strln_loc_invc_dt,
	    pulse_strln_loc_invc_nbr,
	    pulse_strln_loc_invc_tm,
	    pulse_strln_loc_pos_term_nbr,
	    pulse_strln_loc_pos_term_prof,
	    pulse_strln_loc_pos_term_typ,
	    pulse_strln_loc_rdh_rtn,
	    pulse_strln_loc_rdh_sale,
	    pulse_strln_loc_store_id,
	    pulse_strln_loc_tnd_corrtn,
	    pulse_strln_loc_whlsl_trans_typ,
	    pulse_strln_loc_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        CashierNo AS pulse_strln_loc_cshr_nbr, 
        CheckoutBankNo AS pulse_strln_loc_chkout_bank_nbr, 
        DepositPrevTicket AS pulse_strln_loc_depst_prev_tckt_ind, 
        DocType AS pulse_strln_loc_doc_typ, 
        EFTLocation AS pulse_strln_loc_eft_loc, 
        Express_pos AS pulse_strln_loc_exprs_pos, 
        Frgn_cust AS pulse_strln_loc_frgn_cust_ind, 
        FuelMergeRecall AS pulse_strln_loc_fuel_merger_call, 
        Function AS pulse_strln_loc_func, 
        Invoice AS pulse_strln_loc_invc, 
        InvoiceDate AS pulse_strln_loc_invc_dt, 
        InvoiceNo AS pulse_strln_loc_invc_nbr, 
        InvoiceTime AS pulse_strln_loc_invc_tm, 
        POSTermNo AS pulse_strln_loc_pos_term_nbr, 
        POSTermProfile AS pulse_strln_loc_pos_term_prof, 
        POSTermType AS pulse_strln_loc_pos_term_typ, 
        Rdh_return AS pulse_strln_loc_rdh_rtn, 
        Rdh_sale AS pulse_strln_loc_rdh_sale, 
        StoreID AS pulse_strln_loc_store_id, 
        TenderCorrection AS pulse_strln_loc_tnd_corrtn, 
        wholesale_trs_type AS pulse_strln_loc_whlsl_trans_typ, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_loc_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_loc;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_loc" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_loc
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_sprvr 
DECLARE @pulse_strln_sprvr_path string = string.Format("{0}/supervisor.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_sprvr_path) AND FILE.LENGTH(@pulse_strln_sprvr_path) > 0 THEN
    @pulse_strln_sprvr = 
        EXTRACT 
            Opcode int,
            Function int,
            ScanCard short,
            PreliminaryReturn short,
            UseOverrideCode short,
            CreditSale short,
            AuthorizedVoids short,
            ManagerDiscounts short,
            AutoMode short,
            ApprovedbyRPO short,
            SupervisorNumber int,
            prvlg int,
            Keylock short,
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_sprvr_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_sprvr ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_sprvr (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_sprvr_aprv_rpo_ind,
	    pulse_strln_sprvr_auth_void_ind,
	    pulse_strln_sprvr_auto_mode_ind,
	    pulse_strln_sprvr_cr_sale_ind,
	    pulse_strln_sprvr_func,
	    pulse_strln_sprvr_key_lock_ind,
	    pulse_strln_sprvr_mgr_disc_ind,
	    pulse_strln_sprvr_prelim_rtn_ind,
	    pulse_strln_sprvr_scan_crd_ind,
	    pulse_strln_sprvr_nbr,
	    pulse_strln_sprvr_use_ovrd_cd_ind,
	    pulse_strln_sprvr_prvlg,
	    pulse_strln_sprvr_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        ApprovedbyRPO AS pulse_strln_sprvr_aprv_rpo_ind, 
        AuthorizedVoids AS pulse_strln_sprvr_auth_void_ind, 
        AutoMode AS pulse_strln_sprvr_auto_mode_ind, 
        CreditSale AS pulse_strln_sprvr_cr_sale_ind, 
        Function AS pulse_strln_sprvr_func, 
        Keylock AS pulse_strln_sprvr_key_lock_ind, 
        ManagerDiscounts AS pulse_strln_sprvr_mgr_disc_ind, 
        PreliminaryReturn AS pulse_strln_sprvr_prelim_rtn_ind, 
        ScanCard AS pulse_strln_sprvr_scan_crd_ind, 
        SupervisorNumber AS pulse_strln_sprvr_nbr, 
        UseOverrideCode AS pulse_strln_sprvr_use_ovrd_cd_ind, 
        prvlg AS pulse_strln_sprvr_prvlg, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_sprvr_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_sprvr;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_sprvr" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_sprvr
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_eod_strt 
DECLARE @pulse_strln_eod_strt_path string = string.Format("{0}/eodstart.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_eod_strt_path) AND FILE.LENGTH(@pulse_strln_eod_strt_path) > 0 THEN
    @pulse_strln_eod_strt = 
        EXTRACT 
            Opcode int,
            AlertNo int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_eod_strt_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_eod_strt ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_eod_strt (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_eod_strt_alert_nbr,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        AlertNo AS pulse_strln_eod_strt_alert_nbr, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_eod_strt;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_eod_strt" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_eod_strt
    GROUP BY TicketStart_DT ?? TailDate;
END;    


// pulse_strln_sign_on 
DECLARE @pulse_strln_sign_on_path string = string.Format("{0}/signon.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_sign_on_path) AND FILE.LENGTH(@pulse_strln_sign_on_path) > 0 THEN
    @pulse_strln_sign_on = 
        EXTRACT 
            Opcode int,
            Function int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_sign_on_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_sign_on ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_sign_on (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_sign_on_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_sign_on_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_sign_on;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_sign_on" AS tablename,
       TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_sign_on
    GROUP BY TailDate;
END;    


// pulse_strln_sign_off 
DECLARE @pulse_strln_sign_off_path string = string.Format("{0}/signoff.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_sign_off_path) AND FILE.LENGTH(@pulse_strln_sign_off_path) > 0 THEN
    @pulse_strln_sign_off = 
        EXTRACT 
            Opcode int,
            Function int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_sign_off_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_sign_off ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_sign_off (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_sign_off_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_sign_off_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_sign_off;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_sign_off" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_sign_off
    GROUP BY TailDate;           
END;    


// pulse_strln_strt_train 
DECLARE @pulse_strln_strt_train_path string = string.Format("{0}/starttraining.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_strt_train_path) AND FILE.LENGTH(@pulse_strln_strt_train_path) > 0 THEN
    @pulse_strln_strt_train = 
        EXTRACT 
            Opcode int,
            Function int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_strt_train_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_strt_train ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_strt_train (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_strt_train_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_strt_train_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_strt_train;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_strt_train" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_strt_train
    GROUP BY TailDate;
END;    


// pulse_strln_stop_train 
DECLARE @pulse_strln_stop_train_path string = string.Format("{0}/stoptraining.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_stop_train_path) AND FILE.LENGTH(@pulse_strln_stop_train_path) > 0 THEN
    @pulse_strln_stop_train = 
        EXTRACT 
            Opcode int,
            Function int, 
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string
        FROM @pulse_strln_stop_train_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_stop_train ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_stop_train (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_stop_train_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )                       
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_stop_train_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_stop_train;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_stop_train" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_stop_train
    GROUP BY TailDate;
END;    


// pulse_strln_wrng_pswd 
DECLARE @pulse_strln_wrng_pswd_path string = string.Format("{0}/wrongpassword.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_wrng_pswd_path) AND FILE.LENGTH(@pulse_strln_wrng_pswd_path) > 0 THEN
    @pulse_strln_wrng_pswd = 
        EXTRACT 
            Opcode int,
            Function int,
            Mode int,
            CashierNo int,
            Password int,
            TailDoNotProcess short,
            TailBadRecord short,
            TailTrainingMode short,
            TailPosOffline short,
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            Unique int, 
            TailStoreNumber int,
            TailDate DateTime,
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int, 
            TicketStart_DT DateTime?, 
            TicketStart_TM string, 
            Src_T_E string            
        FROM @pulse_strln_wrng_pswd_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    ALTER TABLE pos.storeline.pulse_strln_wrng_pswd ADD IF NOT EXISTS PARTITION(@adfPipelineId);

    INSERT INTO pos.storeline.pulse_strln_wrng_pswd (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_wrng_pswd_cshr_nbr,
	    pulse_strln_wrng_pswd_func,
	    pulse_strln_wrng_pswd_mode,
	    pulse_strln_wrng_pswd,
	    pulse_strln_wrng_pswd_from_evt_ind,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        transaction_id
    )
    PARTITION(@adfPipelineId)
    SELECT DISTINCT 
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        CashierNo AS pulse_strln_wrng_pswd_cshr_nbr, 
        Function AS pulse_strln_wrng_pswd_func, 
        Mode AS pulse_strln_wrng_pswd_mode, 
        Password AS pulse_strln_wrng_pswd, 
        (short)(Src_T_E == "E" ? 1 : 0) AS pulse_strln_wrng_pswd_from_evt_ind, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_wrng_pswd;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount
    )
    PARTITION(@adfPipelineId)
    SELECT "pulse_strln_wrng_pswd" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt
    FROM @pulse_strln_wrng_pswd
    GROUP BY TailDate;
END;    












/*TODO: convert pulse_strln_plu_sale


// pulse_strln_cpn_cross 
DECLARE @pulse_strln_cpn_cross_path string = string.Format("{0}/couponscross.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_cpn_cross_path) AND FILE.LENGTH(@pulse_strln_cpn_cross_path) > 0 THEN
    @pulse_strln_cpn_cross = 
        EXTRACT 


        FROM @pulse_strln_cpn_cross_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    INSERT INTO pos.storeline.pulse_strln_cpn_cross (
        pulse_strln_store_nbr,
        pulse_strln_cshr_nbr,
        pulse_strln_rgstr_nbr,
        pulse_strln_tckt_nbr,
        pulse_strln_tckt_dt,
        pulse_strln_tckt_tm,
        pulse_strln_seq_nbr,
        pulse_strln_oprtn_cd,
        pulse_strln_cpn_cross_shrt_promo_nbr,
        pulse_strln_cpn_cross_rwrd_typ,
        pulse_strln_cpn_cross_promo,
        pulse_strln_cpn_cross_promo_typ,
        pulse_strln_cpn_cross_promo_sub_dept,
        pulse_strln_cpn_cross_promo_plu,
        pulse_strln_cpn_cross_promo_mfr,
        pulse_strln_cpn_cross_promo_mmg,
        pulse_strln_cpn_cross_promo_grp,
        pulse_strln_cpn_cross_promo_dept,
        pulse_strln_cpn_cross_promo_cd,
        pulse_strln_cpn_cross_long_promo_nbr,
        pulse_strln_cpn_cross_func,
        pulse_strln_cpn_cross_dept,
        pulse_strln_cpn_cross_bckt_nbr,
        pulse_strln_bad_rcd_ind,
        pulse_strln_dt,
        pulse_strln_tm,
        pulse_strln_do_not_proc_ind,
        pulse_strln_term_nbr,
        pulse_strln_pos_offln_ind,
        pulse_strln_rtn_tckt,
        pulse_strln_tv,
        pulse_strln_train_mode_ind,
        pulse_strln_void_tckt_ind,
        last_load_ts,
        pipeline_run_id,
        transaction_id
    )                       
    SELECT
        Convert.ToInt32(TailStoreNumber) AS pulse_strln_store_nbr, 
        Convert.ToInt32(TailCashierNo) AS pulse_strln_cshr_nbr, 
        Convert.ToInt32(TailPosNo) AS pulse_strln_rgstr_nbr, 
        Convert.ToInt32(TailTicketNumber) AS pulse_strln_tckt_nbr, 
        DateTime.Parse(TicketStart_DT ?? TailDate) AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        Convert.ToInt32(TailSequenceNumber) AS pulse_strln_seq_nbr, 
        Convert.ToInt32(Opcode) AS pulse_strln_oprtn_cd, 
        Convert.ToInt32(ShortPromoNbr) AS pulse_strln_cpn_cross_shrt_promo_nbr, 
        Convert.ToInt32(RewardType) AS pulse_strln_cpn_cross_rwrd_typ, 
        Convert.ToInt32(Promotion) AS pulse_strln_cpn_cross_promo, 
        Convert.ToInt32(PromoType) AS pulse_strln_cpn_cross_promo_typ, 
        Convert.ToInt32(PromoSubDept) AS pulse_strln_cpn_cross_promo_sub_dept, 
        Convert.ToInt32(PromoPlu) AS pulse_strln_cpn_cross_promo_plu, 
        Convert.ToInt32(PromoManufacture) AS pulse_strln_cpn_cross_promo_mfr, 
        Convert.ToInt32(PromoMMG) AS pulse_strln_cpn_cross_promo_mmg, 
        Convert.ToInt32(PromoGroup) AS pulse_strln_cpn_cross_promo_grp, 
        Convert.ToInt32(PromoDept) AS pulse_strln_cpn_cross_promo_dept, 
        Convert.ToInt64(PromoCode) AS pulse_strln_cpn_cross_promo_cd, 
        Convert.ToInt32(LongPromoNbr) AS pulse_strln_cpn_cross_long_promo_nbr, 
        Convert.ToInt32(Function) AS pulse_strln_cpn_cross_func, 
        Convert.ToInt32(Department) AS pulse_strln_cpn_cross_dept, 
        Convert.ToInt32(BucketNbr) AS pulse_strln_cpn_cross_bckt_nbr, 
        Convert.ToInt16(TailBadRecord) AS pulse_strln_bad_rcd_ind, 
        DateTime.Parse(TailDate) AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        Convert.ToInt16(TailDoNotProcess) AS pulse_strln_do_not_proc_ind, 
        Convert.ToInt32(TailPcNo) AS pulse_strln_term_nbr, 
        Convert.ToInt16(TailPosOffline) AS pulse_strln_pos_offln_ind, 
        Convert.ToInt32(TailReturnTicket) AS pulse_strln_rtn_tckt, 
        Convert.ToInt32(TailTV) AS pulse_strln_tv, 
        Convert.ToInt16(TailTrainingMode) AS pulse_strln_train_mode_ind, 
        Convert.ToInt16(TailVoidTicket) AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts, 
        @adfPipelineId AS pipeline_run_id,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", DateTime.Parse(TicketStart_DT ?? TailDate), (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), Convert.ToInt32(TailStoreNumber), Convert.ToInt32(TailPosNo), Convert.ToInt32(TailTicketNumber))) AS transaction_id
    FROM @pulse_strln_cpn_cross;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount,
        pipeline_run_id
    )
    SELECT "pulse_strln_cpn_cross" AS tablename,
        DateTime.Parse(TicketStart_DT ?? TailDate) AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt,
        @adfPipelineId
    FROM @pulse_strln_cpn_cross
    GROUP BY DateTime.Parse(TicketStart_DT ?? TailDate);
END;    

// pulse_strln_gift_crd_btch_actvtn_det 
DECLARE @pulse_strln_gift_crd_btch_actvtn_det_path string = string.Format("{0}/giftcardbatchactivationdetail.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_gift_crd_btch_actvtn_det_path) AND FILE.LENGTH(@pulse_strln_gift_crd_btch_actvtn_det_path) > 0 THEN
    @pulse_strln_gift_crd_btch_actvtn_det = 
        EXTRACT 


        FROM @pulse_strln_gift_crd_btch_actvtn_det_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    INSERT INTO pos.storeline.pulse_strln_gift_crd_btch_actvtn_det (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_gift_crd_btch_actvtn_det_func,
	    pulse_strln_gift_crd_btch_actvtn_det_idx,
	    pulse_strln_gift_crd_btch_actvtn_det_btch_nbr,
	    pulse_strln_gift_crd_btch_actvtn_det_crd_nbr,
	    pulse_strln_gift_crd_btch_actvtn_det_crd_nbr_length,
	    pulse_strln_gift_crd_btch_actvtn_det_trans_amt,
	    pulse_strln_gift_crd_btch_actvtn_det_plu,
	    pulse_strln_gift_crd_btch_actvtn_det_proc_ind,
	    pulse_strln_gift_crd_btch_actvtn_det_void_ind,
	    pulse_strln_gift_crd_btch_actvtn_det_result,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        pipeline_run_id,
        transaction_id
    )
    SELECT
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TicketStart_DT ?? TailDate AS pulse_strln_tckt_dt, 
        !string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Convert.ToInt32(Function) AS pulse_strln_gift_crd_btch_actvtn_det_func, 
        Convert.ToInt32(Index) AS pulse_strln_gift_crd_btch_actvtn_det_idx, 
        Convert.ToInt32(BatchNo) AS pulse_strln_gift_crd_btch_actvtn_det_btch_nbr, 
        Convert.ToInt64(CardNumber) AS pulse_strln_gift_crd_btch_actvtn_det_crd_nbr, 
        Convert.ToInt32(CardNoLen) AS pulse_strln_gift_crd_btch_actvtn_det_crd_nbr_length, 
        Convert.ToDecimal(TransactionAmount) / Convert.ToDecimal(100) AS pulse_strln_gift_crd_btch_actvtn_det_trans_amt, 
        Plu AS pulse_strln_gift_crd_btch_actvtn_det_plu, 
        Convert.ToInt16(Processed) AS pulse_strln_gift_crd_btch_actvtn_det_proc_ind, 
        Convert.ToInt16(Void) AS pulse_strln_gift_crd_btch_actvtn_det_void_ind, 
        Convert.ToInt32(Result) AS pulse_strln_gift_crd_btch_actvtn_det_result, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind, 
        DateTime.Now AS last_load_ts,
        @adfPipelineId AS pipeline_run_id,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TicketStart_DT ?? TailDate, (!string.IsNullOrEmpty(TicketStart_TM) ? TicketStart_TM : TailTime).Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_gift_crd_btch_actvtn_det;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount,
        pipeline_run_id
    )
    SELECT "pulse_strln_gift_crd_btch_actvtn_det" AS tablename,
        TicketStart_DT ?? TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt,
        @adfPipelineId
    FROM @pulse_strln_gift_crd_btch_actvtn_det
    GROUP BY TicketStart_DT ?? TailDate;
END;  

// pulse_strln_strt_of_day 
DECLARE @pulse_strln_strt_of_day_path string = string.Format("{0}/startofday.txt", @rdsFolderPath).Replace("//", "/");
IF FILE.EXISTS(@pulse_strln_strt_of_day_path) AND FILE.LENGTH(@pulse_strln_strt_of_day_path) > 0 THEN
    @pulse_strln_strt_of_day = 
        EXTRACT 
            Opcode int, 
            Function int, 
            TailDoNotProcess short, 
            TailBadRecord short, 
            TailTrainingMode short, 
            TailPosOffline short, 
            TailVoidTicket short,
            TailReturnTicket int, 
            TailTV int, 
            TailStoreNumber int, 
            TailDate DateTime, 
            TailTime string, 
            TailCashierNo int, 
            TailPcNo int, 
            TailPosNo int, 
            TailTicketNumber int, 
            TailSequenceNumber int            
        FROM @pulse_strln_strt_of_day_path
        USING Extractors.Text(delimiter:'|', skipFirstNRows:1); 

    INSERT INTO pos.storeline.pulse_strln_strt_of_day (
	    pulse_strln_store_nbr,
	    pulse_strln_cshr_nbr,
	    pulse_strln_rgstr_nbr,
	    pulse_strln_tckt_nbr,
	    pulse_strln_tckt_dt,
	    pulse_strln_tckt_tm,
	    pulse_strln_seq_nbr,
	    pulse_strln_oprtn_cd,
	    pulse_strln_strt_of_day_func,
	    pulse_strln_bad_rcd_ind,
	    pulse_strln_dt,
	    pulse_strln_tm,
	    pulse_strln_do_not_proc_ind,
	    pulse_strln_term_nbr,
	    pulse_strln_pos_offln_ind,
	    pulse_strln_rtn_tckt,
	    pulse_strln_tv,
	    pulse_strln_train_mode_ind,
	    pulse_strln_void_tckt_ind,
	    last_load_ts,
        pipeline_run_id,
        transaction_id
    )                       
    SELECT
        TailStoreNumber AS pulse_strln_store_nbr, 
        TailCashierNo AS pulse_strln_cshr_nbr, 
        TailPosNo AS pulse_strln_rgstr_nbr, 
        TailTicketNumber AS pulse_strln_tckt_nbr, 
        TailDate AS pulse_strln_tckt_dt, 
        TailTime AS pulse_strln_tckt_tm, 
        TailSequenceNumber AS pulse_strln_seq_nbr, 
        Opcode AS pulse_strln_oprtn_cd, 
        Function AS pulse_strln_strt_of_day_func, 
        TailBadRecord AS pulse_strln_bad_rcd_ind, 
        TailDate AS pulse_strln_dt, 
        TailTime AS pulse_strln_tm, 
        TailDoNotProcess AS pulse_strln_do_not_proc_ind, 
        TailPcNo AS pulse_strln_term_nbr, 
        TailPosOffline AS pulse_strln_pos_offln_ind, 
        TailReturnTicket AS pulse_strln_rtn_tckt, 
        TailTV AS pulse_strln_tv, 
        TailTrainingMode AS pulse_strln_train_mode_ind, 
        TailVoidTicket AS pulse_strln_void_tckt_ind,
        DateTime.Now AS last_load_ts, 
        @adfPipelineId AS pipeline_run_id,
        Convert.ToDecimal(string.Format("{0:yyyyMMdd}{1}{2,4:0000}{3,4:0000}{4,4:0000}3", TailDate, TailTime.Replace(":", "").Substring(0, 4), TailStoreNumber, TailPosNo, TailTicketNumber)) AS transaction_id
    FROM @pulse_strln_strt_of_day;

    INSERT INTO pos.storeline.rds_load_stats (
        tablename,
	    businessdate,
	    recordcount,
        pipeline_run_id
    )
    SELECT "pulse_strln_strt_of_day" AS tablename,
        TailDate AS businessdate,
        COUNT(TailSequenceNumber) AS transcnt,
        @adfPipelineId
    FROM @pulse_strln_strt_of_day
    GROUP BY TailDate;
END;    
*/
0 comments on commit 439e74c
@Thirapathireddy
Attach files by dragging & dropping, selecting or pasting them.

You’re not receiving notifications from this thread.

    © 2020 GitHub, Inc.
    Terms
    Privacy
    Security
    Status
    Help

    Contact GitHub
    Pricing
    API
    Training
    Blog
    About

