USE [kRAP]
GO
/****** Object:  StoredProcedure [dbo].[KFCU_sp_Insert_TellerTotals_TEST]    Script Date: 9/20/2024 11:43:22 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create PROCEDURE [dbo].[KFCU_sp_Insert_TellerTotals]
    @ProcessDate INT,
    @Branch INT,
    @User INT,
    @Txcount INT

AS
BEGIN
    MERGE [dbo].[KFCU_TellerTotals] AS target
    USING (SELECT @ProcessDate AS ProcessDate, @Branch AS Branch, @User AS [User], @Txcount AS Txcount) AS source
    ON (target.ProcessDate = source.ProcessDate AND target.Branch = source.Branch AND target.[User] = source.[User])
    WHEN MATCHED THEN
        UPDATE SET Txcount = source.Txcount
    WHEN NOT MATCHED THEN
        INSERT (ProcessDate, Branch, [User], Txcount)
        VALUES (source.ProcessDate, source.Branch, source.[User], source.Txcount);
END
