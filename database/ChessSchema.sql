CREATE TABLE Accounts(
    Account_id INTEGER NOT NULL,
    Platform VARCHAR(255) NOT NULL,
    Username VARCHAR(255) NOT NULL,
    PRIMARY KEY(Account_id)
);

CREATE TABLE Openings(
    ECO VARCHAR(3) NOT NULL,
    Opening VARCHAR(255) NOT NULL,
    OpeningMoves VARCHAR(255) NOT NULL,
    PRIMARY KEY(ECO)
);

CREATE TABLE Ratings(
    Account_id INTEGER NOT NULL,
    Datetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    Daily INTEGER NULL,
    Classical INTEGER NULL,
    Rapid INTEGER NULL,
    Blitz INTEGER NULL,
    Bullet INTEGER NULL,
    Ultrabullet INTEGER NULL,
    Chess960 INTEGER NULL,
    Kingofthehill INTEGER NULL,
    Threecheck INTEGER NULL,
    Antichess INTEGER NULL,
    Atomic INTEGER NULL,
    Racingkings INTEGER NULL,
    Crazyhouse INTEGER NULL,
    PRIMARY KEY(Account_id, Datetime),
    FOREIGN KEY(Account_id) REFERENCES Accounts(Account_id)
);

CREATE TABLE Games(
    GameUrl VARCHAR(255) NOT NULL,
    Result VARCHAR(255) NOT NULL,
    Variant VARCHAR(255) NOT NULL,
    ECO VARCHAR(3) NULL,
    Termination VARCHAR(255) NOT NULL,
    PiecesColor VARCHAR(255) NOT NULL,
    Opponent VARCHAR(255) NOT NULL,
    OpponentTitle VARCHAR(255) NOT NULL,
    GameType VARCHAR(255) NOT NULL,
    TimeClass VARCHAR(255) NOT NULL,
    CairoDatetime TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    MyElo INTEGER NOT NULL,
    OpponentElo INTEGER NOT NULL,
    MyRatingDiff INTEGER NOT NULL,
    OpponentRatingDiff INTEGER NOT NULL,
    Account_id INTEGER NOT NULL,
    PRIMARY KEY(GameUrl),
    FOREIGN KEY(ECO) REFERENCES Openings(ECO),
    FOREIGN KEY(Account_id) REFERENCES Accounts(Account_id)
);

CREATE TABLE Moves(
    Gameurl VARCHAR(255) NOT NULL,
    PieceColor VARCHAR(255) NOT NULL,
    Move VARCHAR(255) NOT NULL,
    CLK VARCHAR(255) NOT NULL,
    MoveNumber INTEGER NOT NULL,
    FEN_Before VARCHAR(255) NOT NULL,
    FEN_After VARCHAR(255) NOT NULL,
    PRIMARY KEY(GameUrl, MoveNumber, PieceColor),
    FOREIGN KEY(Gameurl) REFERENCES Games(GameUrl)
);

CREATE TABLE DateTable (
    Date DATE PRIMARY KEY,
    Year INTEGER NOT NULL,
    Quarter INTEGER NOT NULL,
    Month INTEGER NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Day INTEGER NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    Week INTEGER NOT NULL
);

INSERT INTO DateTable(Date, Year, Quarter, Month, MonthName, Day, DayName, Week)
SELECT 
    DateSeries AS Date,
    EXTRACT(YEAR FROM DateSeries) AS Year,
    EXTRACT(QUARTER FROM DateSeries) AS Quarter,
    EXTRACT(MONTH FROM DateSeries) AS Month,
    TO_CHAR(DateSeries, 'Month') AS MonthName,
    EXTRACT(DAY FROM DateSeries) AS Day,
    TO_CHAR(DateSeries, 'Day') AS DayName,
    EXTRACT(WEEK FROM DateSeries) AS Week
FROM 
    GENERATE_SERIES('2020-05-01'::DATE, '2050-12-31'::DATE, '1 day') AS DateSeries;

COPY Openings(ECO, Opening, OpeningMoves)
FROM '/path/to/opening.csv'
DELIMITER ','
CSV HEADER;
