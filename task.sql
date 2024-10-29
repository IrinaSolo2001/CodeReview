CREATE PROCEDURE syn.usp_ImportFileCustomerSeasonal  -- Ошибка 1: PascalCase для названия процедуры, предпочтительнее ImportFileCustomerSeasonal
    @ID_Record INT
AS
SET NOCOUNT ON
BEGIN
    -- Ошибка 2: Отсутствует комментарий, объясняющий цель процедуры и параметры
    DECLARE @RowCount INT = (SELECT COUNT(*) FROM syn.SA_CustomerSeasonal)
    DECLARE @ErrorMessage VARCHAR(MAX)

-- Проверка на корректность загрузки
    IF NOT EXISTS (  -- Ошибка 3: IF должен быть в верхнем регистре
    SELECT 1
    FROM syn.ImportFile AS f
    WHERE f.ID = @ID_Record
        AND f.FlagLoaded = CAST(1 AS BIT)  -- Ошибка 4: CAST в верхнем регистре
    )
    BEGIN
        SET @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

        RAISERROR(@ErrorMessage, 3, 1)  -- Ошибка 5: RAISERROR в верхнем регистре
        RETURN
    END

-- Чтение из слоя временных данных
    SELECT
        c.ID AS ID_dbo_Customer,  -- Ошибка 6: пробел после запятой
        cst.ID AS ID_CustomerSystemType,
        s.ID AS ID_Season,
        CAST(cs.DateBegin AS DATE) AS DateBegin,  -- Ошибка 7: CAST в верхнем регистре
        CAST(cs.DateEnd AS DATE) AS DateEnd,
        c_dist.ID AS ID_dbo_CustomerDistributor,
        CAST(ISNULL(cs.FlagActive, 0) AS BIT) AS FlagActive
    INTO #CustomerSeasonal
    FROM syn.SA_CustomerSeasonal cs
        JOIN dbo.Customer AS c ON c.UID_DS = cs.UID_DS_Customer
            AND c.ID_mapping_DataSource = 1
        JOIN dbo.Season AS s ON s.Name = cs.Season
        JOIN dbo.Customer AS c_dist ON c_dist.UID_DS = cs.UID_DS_CustomerDistributor
            AND cd.ID_mapping_DataSource = 1  -- Ошибка 8: неверный алиас таблицы, должен быть c_dist
        JOIN syn.CustomerSystemType AS cst ON cs.CustomerSystemType = cst.Name
    WHERE TRY_CAST(cs.DateBegin AS DATE) IS NOT NULL  -- Ошибка 9: TRY_CAST в верхнем регистре
        AND TRY_CAST(cs.DateEnd AS DATE) IS NOT NULL
        AND TRY_CAST(ISNULL(cs.FlagActive, 0) AS BIT) IS NOT NULL

-- Определение некорректных записей
    SELECT
        cs.*
        ,CASE
            WHEN c.ID IS NULL THEN 'UID клиента отсутствует в справочнике "Клиент"'
            WHEN c_dist.ID IS NULL THEN 'UID дистрибьютора отсутствует в справочнике "Клиент"'
            WHEN s.ID IS NULL THEN 'Сезон отсутствует в справочнике "Сезон"'
            WHEN cst.ID IS NULL THEN 'Тип клиента отсутствует в справочнике "Тип клиента"'
            WHEN TRY_CAST(cs.DateBegin AS DATE) IS NULL THEN 'Невозможно определить Дату начала'  -- Ошибка 10: TRY_CAST в верхнем регистре
            WHEN TRY_CAST(cs.DateEnd AS DATE) IS NULL THEN 'Невозможно определить Дату окончания'
            WHEN TRY_CAST(ISNULL(cs.FlagActive, 0) AS BIT) IS NULL THEN 'Невозможно определить Активность'
        END AS Reason
    INTO #BadInsertedRows
    FROM syn.SA_CustomerSeasonal AS cs
    LEFT JOIN dbo.Customer AS c ON c.UID_DS = cs.UID_DS_Customer
        AND c.ID_mapping_DataSource = 1
    LEFT JOIN dbo.Customer AS c_dist ON c_dist.UID_DS = cs.UID_DS_CustomerDistributor 
        AND c_dist.ID_mapping_DataSource = 1
    LEFT JOIN dbo.Season AS s ON s.Name = cs.Season
    LEFT JOIN syn.CustomerSystemType AS cst ON cst.Name = cs.CustomerSystemType
    WHERE cc.ID IS NULL  -- Ошибка 11: несуществующий алиас "cc"
        OR cd.ID IS NULL  -- Ошибка 12: несуществующий алиас "cd"
        OR s.ID IS NULL
        OR cst.ID IS NULL
        OR TRY_CAST(cs.DateBegin AS DATE) IS NULL
        OR TRY_CAST(cs.DateEnd AS DATE) IS NULL
        OR TRY_CAST(ISNULL(cs.FlagActive, 0) AS BIT) IS NULL

-- Обработка данных из файла
    MERGE INTO syn.CustomerSeasonal AS cs
    USING (
        SELECT
            cs_temp.ID_dbo_Customer,
            cs_temp.ID_CustomerSystemType,
            cs_temp.ID_Season,
            cs_temp.DateBegin,
            cs_temp.DateEnd,
            cs_temp.ID_dbo_CustomerDistributor,
            cs_temp.FlagActive
        FROM #CustomerSeasonal AS cs_temp
    ) AS s ON s.ID_dbo_Customer = cs.ID_dbo_Customer
        AND s.ID_Season = cs.ID_Season
        AND s.DateBegin = cs.DateBegin
    WHEN MATCHED 
        AND t.ID_CustomerSystemType <> s.ID_CustomerSystemType THEN
        UPDATE
        SET
            ID_CustomerSystemType = s.ID_CustomerSystemType,
            DateEnd = s.DateEnd,
            ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor,
            FlagActive = s.FlagActive
    WHEN NOT MATCHED THEN
        INSERT (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
        VALUES (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive)

-- Информационное сообщение
    BEGIN
        SELECT @ErrorMessage = CONCAT('Обработано строк: ', @RowCount)  -- Ошибка 13: CONCAT в верхнем регистре

        RAISERROR(@ErrorMessage, 1, 1)  -- Ошибка 14: RAISERROR в верхнем регистре

-- Формирование таблицы для отчетности
        SELECT TOP 100
            Season AS 'Сезон',
            UID_DS_Customer AS 'UID Клиента',
            Customer AS 'Клиент',
            CustomerSystemType AS 'Тип клиента',
            UID_DS_CustomerDistributor AS 'UID Дистрибьютора',
            CustomerDistributor AS 'Дистрибьютор',
            ISNULL(FORMAT(TRY_CAST(DateBegin AS DATE), 'dd.MM.yyyy', 'ru-RU'), DateBegin) AS 'Дата начала',  -- Ошибка 15: FORMAT и TRY_CAST в верхнем регистре
            ISNULL(FORMAT(TRY_CAST(DateEnd AS DATE), 'dd.MM.yyyy', 'ru-RU'), DateEnd) AS 'Дата окончания',
            FlagActive AS 'Активность',
            Reason AS 'Причина'
        FROM #BadInsertedRows

        RETURN
    END
END

-- Ошибка 16: Имя процедуры не отражает назначение, предпочтительнее ImportCustomerSeasonalData.
-- Ошибка 17: Отсутствие транзакций — при обработке данных рекомендуется использовать блоки BEGIN TRANSACTION ... COMMIT.
-- Ошибка 18: Временные таблицы #CustomerSeasonal и #BadInsertedRows не удаляются после использования. Нужно добавить DROP TABLE #CustomerSeasonal, #BadInsertedRows в конце процедуры.
