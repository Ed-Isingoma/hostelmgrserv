const { Pool } = require('pg');
require('dotenv').config();

const credentials = {
  apiKey: process.env.AFRSTK_API,
  username: process.env.USRNAME
}
const AfricasTalking = require('africastalking')(credentials)

// PostgreSQL connection pool

const pool = new Pool({
  user: process.env.USERNAME,
  host: process.env.HOST,
  database: process.env.DB_NAME,
  password: process.env.PASSWORD,
  port: 5432,
  ssl:true
});

// const pool = new Pool({
//   connectionString: process.env.DATABASE_URL, 
//   ssl: {
//     rejectUnauthorized: false, // Required for some cloud databases (like Render)
//   },
// });

async function initDb() {
  try {
    const client = await pool.connect();

    const queries = [
      `CREATE TABLE IF NOT EXISTS Account (
        accountId SERIAL PRIMARY KEY,
        username TEXT NOT NULL CHECK(username NOT LIKE '% %'),
        password TEXT NOT NULL CHECK(length(password) >= 4),
        approved BOOLEAN NOT NULL DEFAULT false,
        role TEXT NOT NULL CHECK(role IN ('admin', 'custodian')),
        deleted BOOLEAN NOT NULL DEFAULT false
      );`,

      `CREATE TABLE IF NOT EXISTS Tenant (
        tenantId SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        gender TEXT CHECK(gender IN ('male', 'female')),
        age INTEGER,
        course TEXT,
        ownContact TEXT,
        nextOfKin TEXT,
        kinContact TEXT,
        deleted BOOLEAN NOT NULL DEFAULT false
      );`,

      `CREATE TABLE IF NOT EXISTS Room (
        roomId SERIAL PRIMARY KEY,
        levelNumber INTEGER NOT NULL,
        roomName TEXT NOT NULL,
        deleted BOOLEAN NOT NULL DEFAULT false
      );`,

      `CREATE TABLE IF NOT EXISTS BillingPeriodName (
        periodNameId SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        startingDate DATE NOT NULL,
        endDate DATE NOT NULL,
        costSingle INTEGER,
        costDouble INTEGER,
        deleted BOOLEAN NOT NULL DEFAULT false
      );`,

      `CREATE TABLE IF NOT EXISTS BillingPeriod (
        periodId SERIAL PRIMARY KEY,
        periodNameId INTEGER NOT NULL,
        tenantId INTEGER NOT NULL,
        roomId INTEGER NOT NULL,
        demandNoticeDate DATE,
        agreedPrice INTEGER NOT NULL,
        ownStartingDate DATE,
        ownEndDate DATE,
        periodType TEXT NOT NULL CHECK(periodType IN ('single', 'double')),
        deleted BOOLEAN NOT NULL DEFAULT false,
        FOREIGN KEY (tenantId) REFERENCES Tenant(tenantId),
        FOREIGN KEY (roomId) REFERENCES Room(roomId),
        FOREIGN KEY (periodNameId) REFERENCES BillingPeriodName(periodNameId)
      );`,

      `CREATE TABLE IF NOT EXISTS Transactionn (
        transactionId SERIAL PRIMARY KEY,
        periodId INTEGER NOT NULL,
        date DATE NOT NULL,
        amount INTEGER NOT NULL,
        deleted BOOLEAN NOT NULL DEFAULT false,
        FOREIGN KEY (periodId) REFERENCES BillingPeriod(periodId)
      );`,

      `CREATE TABLE IF NOT EXISTS MiscExpense (
        expenseId SERIAL PRIMARY KEY,
        description TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 1,
        amount INTEGER NOT NULL,
        periodNameId INTEGER NOT NULL,
        operator INTEGER NOT NULL,
        deleted BOOLEAN NOT NULL DEFAULT false,
        date DATE NOT NULL,
        FOREIGN KEY (periodNameId) REFERENCES BillingPeriodName(periodNameId),
        FOREIGN KEY (operator) REFERENCES Account(accountId)
      );`
    ];

    for (const query of queries) {
      await client.query(query);
    }

    console.log('All tables created or verified successfully.');
    client.release();
  } catch (err) {
    console.error('Error initializing database:', err);
  }
}

function executeQuery(quer, par = []) {
  let { query, params } = prepareQuery(quer, par)

  console.log('Going to execute query:', query);
  console.log('Parameters are:', params);

  query = query.trim().replace(/;$/, '');  // Remove trailing semicolon if present

  // Add RETURNING * for queries that modify data
  if (/^(INSERT|UPDATE|DELETE)/i.test(query)) {
    query += ' RETURNING *';
  }

  return pool.query(query, params)
    .then(result => {
      if (result.rows.length > 0) {
        console.log('The results are:', result.rows);
      } else {
        console.log('No rows returned: row count is', result.rowCount);
      }
      return result.rows
    })
    .catch(err => {
      console.error('Error executing query:', err);
      throw err;
    });
}

async function wipeTables() {
  const dropQueries = [
    `DROP TABLE IF EXISTS MiscExpense CASCADE;`,
    `DROP TABLE IF EXISTS Transactionn CASCADE;`,
    `DROP TABLE IF EXISTS BillingPeriod CASCADE;`,
    `DROP TABLE IF EXISTS BillingPeriodName CASCADE;`,
    `DROP TABLE IF EXISTS Room CASCADE;`,
    `DROP TABLE IF EXISTS Tenant CASCADE;`,
    `DROP TABLE IF EXISTS Account CASCADE;`
  ];

  try {
    for (const query of dropQueries) {
      await executeQuery(query);
      console.log('Table wiped successfully.');
    }
    console.log('All tables wiped successfully.');
  } catch (error) {
    console.error('Error wiping tables:', error);
  }
}

function prepareQuery(query, params) {
  let paramIndex = 1;
  let resultQuery = '';

  const parts = query.split('?');
  resultQuery = parts
    .map((part, index) => (index < parts.length - 1 ? `${part}$${paramIndex++}` : part))
    .join('');

  return { query: resultQuery, params };
}

async function initializeTrigger() {
  await initDb()
  const checkQuery = `SELECT COUNT(*) AS count FROM Account`;
  try {
    const rows = await executeQuery(checkQuery);
    const isEmpty = rows[0].count == 0
    if (isEmpty) {
      const makeAdmin = `INSERT INTO Account (username, password, role, approved) VALUES (?, ?, ?, ?)`
      const params = ['admin', '2024admin', 'admin', true]
      const adminId = await executeQuery(makeAdmin, params);
      console.log(`Admin account added with ID ${adminId}`);
      await insertDefaultBillingPeriodNames();
      await createDefaultRooms()
    } else {
      console.log("InitTrigger already has records. No insertion needed.");
    }
  } catch (error) {
    console.error("Error initializing InitTrigger:", error);
  }
}

function login(username, password) {
  const query = `SELECT * FROM Account WHERE username = ? AND password = ? AND approved = true AND deleted = false`;
  const params = [username, password];
  return executeQuery(query, params);
}

async function createAccount(username, password, role = 'custodian') {
  const query = `INSERT INTO Account (username, password, role) VALUES (?, ?, ?)`;
  const params = [username, password, role];
  return await executeQuery(query, params);
}

async function insertDefaultBillingPeriodNames() {
  const billingPeriodNames = [
    {
      name: "Semester 1 2024/2025",
      startingDate: "2024-08-03",
      endDate: "2024-12-08",
      costSingle: 1300000,
      costDouble: 650000
    },
    {
      name: "Semester 2 2024/2025",
      startingDate: "2025-01-18",
      endDate: "2025-06-15",
      costSingle: 1300000,
      costDouble: 650000
    },
    {
      name: "Recess 2024/2025",
      startingDate: "2025-06-22",
      endDate: "2025-08-24",
      costSingle: 1300000,
      costDouble: 650000
    }
  ];
  try {
    for (const period of billingPeriodNames) {
      const periodId = await createBillingPeriodName(period);
      console.log(`Billing period '${period.name}' inserted with ID: ${periodId}`);
    }
  } catch (error) {
    console.error("Error inserting billing periods:", error);
  }
}

async function createBillingPeriodName(periodName) {
  const query = `INSERT INTO BillingPeriodName (name, startingDate, endDate, costSingle, costDouble) VALUES (?, ?, ?, ?, ?)`;
  const params = [
    periodName.name,
    periodName.startingDate,
    periodName.endDate,
    periodName.costSingle || null,
    periodName.costDouble || null
  ];
  return await executeQuery(query, params);
}

async function createBillingPeriod(billingPeriod, periodNameId, roomId, tenantId) {
  const query = `INSERT INTO BillingPeriod (periodNameId, tenantId, roomId, demandNoticeDate, agreedPrice, periodType, ownStartingDate, ownEndDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
  const params = [
    periodNameId,
    tenantId,
    roomId,
    billingPeriod.demandNoticeDate || null,
    billingPeriod.agreedPrice,
    billingPeriod.periodType,
    billingPeriod.ownStartingDate || null,
    billingPeriod.ownEndDate || null
  ];
  return await executeQuery(query, params);
}

async function createTenant(tenant) {
  const query = `INSERT INTO Tenant (name, gender, age, course, ownContact, nextOfKin, kinContact) VALUES (?, ?, ?, ?, ?, ?, ?)`;
  const params = [
    tenant.name,
    tenant.gender,
    tenant.age || null,
    tenant.course || null,
    tenant.ownContact || null,
    tenant.nextOfKin || null,
    tenant.kinContact || null
  ];
  return await executeQuery(query, params);
}

async function createMiscExpense(expense, operator, periodNameId) {
  const query = `INSERT INTO MiscExpense (description, quantity, amount, operator, date, periodNameId) VALUES (?, ?, ?, ?, ?, ?)`;
  const params = [
    expense.description,
    expense.quantity,
    expense.amount,
    operator,
    expense.date,
    periodNameId
  ];
  return await executeQuery(query, params);
}

async function createTransaction(transaction, periodId) {
  const query = `INSERT INTO Transactionn (periodId, date, amount) VALUES (?, ?, ?)`;
  const params = [periodId, transaction.date, transaction.amount];
  return await executeQuery(query, params);
}

async function updateRoom(roomId, updatedFields) {
  let query = 'UPDATE Room SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE roomId = ?';
  values.push(roomId);

  return await executeQuery(query, values);
}

async function updateTransaction(transactionId, updatedFields) {
  let query = 'UPDATE Transactionn SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE transactionId = ?';
  values.push(transactionId);

  return await executeQuery(query, values);
}

async function updateBillingPeriod(periodId, updatedFields) {
  let query = 'UPDATE BillingPeriod SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE periodId = ?';
  values.push(periodId);

  return await executeQuery(query, values);
}

async function updateBillingPeriodName(periodNameId, updatedFields) {
  let query = 'UPDATE BillingPeriodName SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE periodNameId = ?';
  values.push(periodNameId);

  return await executeQuery(query, values);
}

async function updateAccount(accountId, updatedFields) {
  let query = 'UPDATE Account SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE accountId = ?';
  values.push(accountId);

  return await executeQuery(query, values);
}

async function updateTenant(tenantId, updatedFields) {
  let query = 'UPDATE Tenant SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE tenantId = ?';
  values.push(tenantId);

  return await executeQuery(query, values);
}

async function updateMiscExpense(expenseId, updatedFields) {
  let query = 'UPDATE MiscExpense SET ';
  const values = [];

  Object.keys(updatedFields).forEach((field, index) => {
    query += `${field} = ?${index < Object.keys(updatedFields).length - 1 ? ',' : ''} `;
    values.push(updatedFields[field]);
  });

  query += 'WHERE expenseId = ?';
  values.push(expenseId);

  return await executeQuery(query, values);
}

//getters

async function getPotentialTenantRoomsByGender(gender, levelNumber, periodNameId) {
  const query = `
  SELECT r.roomId, r.roomName
  FROM Room r
  LEFT JOIN BillingPeriod bp ON r.roomId = bp.roomId AND bp.periodNameId = ?
  LEFT JOIN Tenant t ON bp.tenantId = t.tenantId
  WHERE r.levelNumber = ?
    AND r.deleted = false
    AND (
      bp.periodId IS NULL  -- Room is not occupied for the specified period
      OR (
        bp.periodId IS NOT NULL 
        AND bp.periodType = 'double'
        AND t.gender = ? -- Occupant is of the matching gender
      )
    )
  GROUP BY r.roomId, r.roomName
  HAVING COUNT(t.tenantId) <= 1;  -- Room has zero or one occupants
`;

  const params = [periodNameId, levelNumber, gender];
  return await executeQuery(query, params)

}

function getMiscExpensesByDate(startDate, endDate = null) {
  let query = `
  SELECT MiscExpense.*, Account.username AS operatorName
  FROM MiscExpense
  JOIN Account ON MiscExpense.operator = Account.accountId
  WHERE MiscExpense.date >= ? 
    AND MiscExpense.deleted = false
    AND Account.deleted = false
`;
  const params = [startDate];

  if (endDate) {
    query += ' AND date <= ?';
    params.push(endDate);
  }
  return executeQuery(query, params);
}

function getMiscExpensesForBillingPeriodName(periodNameId) {
  let query = `
  SELECT MiscExpense.*, Account.username AS operatorName
  FROM MiscExpense
  JOIN Account ON MiscExpense.operator = Account.accountId
  WHERE MiscExpense.periodNameId = ?
    AND MiscExpense.deleted = false
    AND Account.deleted = false
  `;
  const params = [periodNameId];

  return executeQuery(query, params);
}

function getMostRecentTransaction(periodId) {
  const query = `SELECT * FROM Transactionn WHERE periodId = ? AND deleted = false ORDER BY date DESC LIMIT 1`;
  return executeQuery(query, [periodId]);
}

function getTransactions(periodId) {
  const query = `SELECT * FROM Transactionn WHERE periodId = ? AND deleted = false ORDER BY date DESC`;
  return executeQuery(query, [periodId]);
}

function getAccountsDeadAndLiving() {
  const query = `SELECT * FROM Account WHERE deleted = false`;
  return executeQuery(query);
}

function getLevels() {
  const query = `SELECT DISTINCT levelNumber FROM Room WHERE deleted = false`;
  return executeQuery(query);
}

function getAllRooms() {
  const query = `SELECT * FROM Room WHERE deleted = false`;
  return executeQuery(query);
}

function getBillingPeriodNames() {
  const query = `SELECT * FROM BillingPeriodName WHERE deleted = false`
  return executeQuery(query);
}

async function getRoomsAndOccupancyByLevel(levelNumber, periodNameId) {
  const query = `
    SELECT Room.roomId, Room.roomName, Room.levelNumber,
      CASE 
        WHEN COUNT(BillingPeriod.periodId) = 2 THEN 100
        WHEN COUNT(BillingPeriod.periodId) = 1 THEN 50
        WHEN COUNT(BillingPeriod.periodId) = 0 THEN 0
        ELSE 101
      END AS occupancyRate
    FROM Room
    LEFT JOIN BillingPeriod ON Room.roomId = BillingPeriod.roomId 
      AND BillingPeriod.periodNameId = ? AND (BillingPeriod.ownEndDate IS NULL OR BillingPeriod.ownEndDate >= CURRENT_DATE)
      AND BillingPeriod.deleted = false
    WHERE Room.levelNumber = ? 
      AND Room.deleted = false
    GROUP BY Room.roomId, Room.roomName, Room.levelNumber
  `;
  const params = [periodNameId, levelNumber];
  return executeQuery(query, params)
}

function getTenantsByLevel(levelNumber, periodNameId) {
  const query = `
    SELECT Tenant.* 
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    JOIN Room ON BillingPeriod.roomId = Room.roomId
    WHERE BillingPeriod.periodNameId = ? 
      AND Room.levelNumber = ?
      AND Tenant.deleted = false 
      AND BillingPeriod.deleted = false
      AND Room.deleted = false
  `;
  return executeQuery(query, [periodNameId, levelNumber]);
}

async function getTenantsAndOwingAmtByRoom(roomId, periodNameId) {
  const query = `
    SELECT Tenant.name, Tenant.gender,
      BillingPeriod.agreedPrice - COALESCE(SUM(Transactionn.amount), 0) AS owingAmount, CASE WHEN BillingPeriod.ownEndDate IS NOT NULL THEN 'Yes' ELSE 'No' END AS paysMonthly
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
    WHERE BillingPeriod.roomId = ? 
      AND BillingPeriod.periodNameId = ?
      AND (BillingPeriod.ownEndDate IS NULL OR BillingPeriod.ownEndDate >= CURRENT_DATE)
      AND Tenant.deleted = false 
      AND BillingPeriod.deleted = false
    GROUP BY Tenant.tenantId, Tenant.name, Tenant.gender, BillingPeriod.ownEndDate, BillingPeriod.agreedPrice
  `;
  const params = [roomId, periodNameId];
  return await executeQuery(query, params);
}

function getAllTenants(periodNameId) {
  const query = `
    SELECT Tenant.* 
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    WHERE BillingPeriod.periodNameId = ? AND Tenant.deleted = false AND BillingPeriod.deleted = false
  `;
  return executeQuery(query, [periodNameId]);
}

function getAllTenantsNameAndId(periodNameId) {
  const query = `
    SELECT Tenant.tenantId, Tenant.name 
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    WHERE BillingPeriod.periodNameId = ? AND Tenant.deleted = false AND BillingPeriod.deleted = false
  `;
  return executeQuery(query, [periodNameId]);
}

function getTenantsByBillingPeriodName(periodNameId) {
  const query = `
    SELECT Tenant.* 
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    WHERE BillingPeriod.periodNameId = ? 
      AND Tenant.deleted = false 
      AND BillingPeriod.deleted = false
  `;
  return executeQuery(query, [periodNameId]);
}

function getMiscExpenseById(miscExpenseId) {
  const query = `SELECT * FROM MiscExpense WHERE miscExpenseId = ? AND deleted = false`;
  return executeQuery(query, [miscExpenseId]);
}

function getTransactionById(transactionId) {
  const query = `SELECT * FROM Transactionn WHERE transactionId = ? AND deleted = false`;
  return executeQuery(query, [transactionId]);
}

function getAccountById(accountId) {
  const query = `SELECT * FROM Account WHERE accountId = ? AND deleted = false`;
  return executeQuery(query, [accountId]);
}

function getMonthliesFor(tenantId) {
  const query = `SELECT BillingPeriod.*, Room.roomName 
  FROM BillingPeriod JOIN Room ON BillingPeriod.roomId = Room.roomId
  WHERE ownStartingDate IS NOT NULL
  AND ownEndDate IS NOT NULL
  AND tenantId = ?
  AND BillingPeriod.deleted = false`

  return executeQuery(query, [tenantId])
}

function getBillingPeriodById(periodId) {
  const query = `SELECT * FROM BillingPeriod WHERE periodId = ? AND deleted = false`;
  return executeQuery(query, [periodId]);
}

function getBillingPeriodBeingPaidFor(tenantId, periodNameId) {
  const query = `SELECT BillingPeriod.*, Room.roomName
  FROM BillingPeriod JOIN Room ON BillingPeriod.roomId = Room.roomId
  WHERE tenantId = ? 
  AND periodNameId = ?
  AND ownEndDate IS NULL
  AND BillingPeriod.deleted = false `;
  return executeQuery(query, [tenantId, periodNameId]);
}

function getOnlyTenantsWithOwingAmt(periodNameId) {
  const query = `
    SELECT Tenant.name, Tenant.ownContact, Tenant.tenantId, Room.roomName, BillingPeriod.agreedPrice, MAX(Transactionn.date) AS lastPaymentDate, BillingPeriod.demandNoticeDate,
      BillingPeriod.agreedPrice - COALESCE(SUM(Transactionn.amount), 0) AS owingAmount, CASE WHEN BillingPeriod.ownEndDate IS NOT NULL THEN 'Yes' ELSE 'No' END AS paysMonthly
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    JOIN Room on BillingPeriod.roomId = Room.roomId
    LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId 
    WHERE BillingPeriod.periodNameId = ? 
      AND Tenant.deleted = false 
      AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
      AND BillingPeriod.deleted = false
    GROUP BY Tenant.tenantId, Tenant.name, Tenant.ownContact, Room.roomName, BillingPeriod.agreedPrice, BillingPeriod.demandNoticeDate, BillingPeriod.ownEndDate
    HAVING BillingPeriod.agreedPrice - COALESCE(SUM(Transactionn.amount), 0) > 0
  `;
  const params = [periodNameId];
  return executeQuery(query, params);
}

function getTenantsPlusOutstandingBalanceAll(periodNameId) {
  const query = `
    SELECT Tenant.name, Tenant.gender, Tenant.tenantId, Tenant.ownContact, Room.roomName,
      BillingPeriod.agreedPrice - COALESCE(SUM(Transactionn.amount), 0) AS owingAmount, BillingPeriod.ownEndDate
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    JOIN Room on BillingPeriod.roomId = Room.roomId
    LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
    WHERE BillingPeriod.periodNameId = ? 
      AND (BillingPeriod.ownEndDate IS NULL OR BillingPeriod.ownEndDate >= CURRENT_DATE)
      AND Tenant.deleted = false 
      AND BillingPeriod.deleted = false
      AND Room.deleted = false
    GROUP BY Tenant.name, Tenant.gender, Tenant.tenantId, Tenant.ownContact, BillingPeriod.agreedPrice, Room.roomName, BillingPeriod.ownEndDate
  `;

  const params = [periodNameId];
  return executeQuery(query, params);
}

async function getFullTenantProfile(tenantId) {
  const query = `
    SELECT 
      t.*, b.periodId, b.periodNameId, 
      b.demandNoticeDate, b.agreedPrice, b.periodType,
      b.ownStartingDate, b.ownEndDate, tr.transactionId, tr.date, tr.amount, r.*
    FROM Tenant t
    LEFT JOIN BillingPeriod b ON t.tenantId = b.tenantId
    LEFT JOIN Room r ON b.roomId = r.roomId
    LEFT JOIN Transactionn tr ON b.periodId = tr.periodId
    WHERE t.tenantId = ?
  `;

  const results = await executeQuery(query, [tenantId]);

  const fullTenantProfile = {};
  results.forEach(row => {
    if (!fullTenantProfile.tenantId) {
      fullTenantProfile.tenantId = row.tenantid;
      fullTenantProfile.name = row.name;
      fullTenantProfile.gender = row.gender
      fullTenantProfile.age = row.age
      fullTenantProfile.course = row.course
      fullTenantProfile.ownContact = row.owncontact;
      fullTenantProfile.nextOfKin = row.nextofkin
      fullTenantProfile.kinContact = row.kincontact
      fullTenantProfile.billingPeriods = [];
    }

    const billingPeriod = fullTenantProfile.billingPeriods.find(
      bp => bp.periodid === row.periodid
    )

    if (!billingPeriod) {
      fullTenantProfile.billingPeriods.push({
        periodId: row.periodid,
        periodNameId: row.periodnameid,
        roomId: row.roomid,
        agreedPrice: row.agreedprice,
        periodType: row.periodtype,
        demandNoticeDate: row.demandnoticedate,
        ownStartingDate: row.ownStartingDate,
        ownEndDate: row.ownenddate,
        room: {
          roomId: row.roomid,
          roomName: row.roomname,
          levelNumber: row.levelnumber
        },
        transactions: row.transactionid
          ? [
            {
              transactionId: row.transactionid,
              amount: row.amount,
              date: row.date
            },
          ]
          : [],
      });
    } else if (row.transactionid) {
      billingPeriod.transactions.push({
        transactionId: row.transactionid,
        amount: row.amount,
        date: row.date
      });
    }
  });

  return fullTenantProfile;
}

function searchTenantByName(name) {
  const query = `
    SELECT 
      t.*, 
      r.roomName, 
      r.levelNumber, 
      bpn.name AS billingPeriodName
    FROM Tenant t
    JOIN BillingPeriod bp ON t.tenantId = bp.tenantId
    JOIN Room r ON bp.roomId = r.roomId
    JOIN BillingPeriodName bpn ON bp.periodNameId = bpn.periodNameId
    WHERE t.name LIKE ? AND t.deleted = false AND bp.deleted = false AND r.deleted = false
  `;
  return executeQuery(query, [`%${name}%`]);
}

function searchTenantNameAndId(nm) {
  const name = nm.toLowerCase();
  const query = `
    SELECT 
      t.tenantId, 
      t.name
    FROM Tenant t
    WHERE LOWER(t.name) LIKE ? AND t.deleted = false
  `;
  return executeQuery(query, [`%${name}%`]);
}

function searchRoomByNamePart(name) {
  const query = `
    SELECT 
      r.roomId, 
      r.roomName
    FROM Room r
    WHERE r.roomName LIKE ? AND r.deleted = false
  `;
  return executeQuery(query, [`%${name}%`]);
}

function getTenantsOfBillingPeriodXButNotY(periodNameId1, periodNameId2) {
  const query = `
    SELECT Tenant.*
    FROM Tenant
    JOIN BillingPeriod bp1 ON Tenant.tenantId = bp1.tenantId
    WHERE bp1.periodNameId = ?
      AND Tenant.deleted = false
      AND bp1.deleted = false
      AND Tenant.tenantId NOT IN (
        SELECT bp2.tenantId
        FROM BillingPeriod bp2
        WHERE bp2.periodNameId = ? AND bp2.deleted = false
      )
  `;
  const params = [periodNameId1, periodNameId2];
  return executeQuery(query, params);
}

function getOlderTenantsThan(periodNameId) {
  let query = `
    SELECT Tenant.tenantId, Tenant.name, Tenant.gender, Tenant.ownContact, Room.roomName, BillingPeriodName.name AS lastSeen,
    BillingPeriod.agreedPrice - COALESCE(SUM(Transactionn.amount), 0) AS owingAmount, CASE WHEN BillingPeriod.ownEndDate IS NOT NULL THEN 'Yes' ELSE 'No' END AS paysMonthly
    FROM Tenant
    JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
    JOIN Room ON BillingPeriod.roomId = Room.roomId
    JOIN BillingPeriodName ON BillingPeriod.periodNameId = BillingPeriodName.periodNameId
    LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
    WHERE (
      BillingPeriodName.startingDate < (
          SELECT startingDate FROM BillingPeriodName WHERE periodNameId = ?
      ) AND Tenant.tenantId NOT IN (
        SELECT tenantId
        FROM BillingPeriod
        WHERE periodNameId = ?
        AND deleted = false
      ) 
    ) OR (
      BillingPeriodName.periodNameId = ? 
      AND BillingPeriod.ownEndDate IS NOT NULL 
      AND BillingPeriod.ownEndDate < CURRENT_DATE
    )
    AND Tenant.deleted = false
    AND BillingPeriod.deleted = false
    AND BillingPeriodName.deleted = false
    AND Room.deleted = false
    
    GROUP BY Tenant.tenantId, Tenant.name, Tenant.gender, Tenant.ownContact, Room.roomName, BillingPeriod.agreedPrice, BillingPeriodName.name, BillingPeriod.ownEndDate
    ORDER BY owingAmount DESC, Tenant.name;
  `

  const params = [periodNameId, periodNameId, periodNameId];
  return executeQuery(query, params);
}

async function getTransactionsByPeriodNameIdWithMetaData(periodNameId) {
  const query = `
    SELECT 
      Transactionn.date AS date,
      Transactionn.amount AS amount,
      Tenant.name AS tenantName,
      Tenant.tenantId AS tenantId,
      Tenant.ownContact AS contact,
      Room.roomName,
      BillingPeriodName.name AS billingPeriodName,
      BillingPeriod.agreedPrice AS agreedPrice,
      Transactionn.transactionId AS transactionId
    FROM Transactionn
    JOIN BillingPeriod ON Transactionn.periodId = BillingPeriod.periodId
    JOIN Tenant ON BillingPeriod.tenantId = Tenant.tenantId
    JOIN Room ON BillingPeriod.roomId = Room.roomId
    JOIN BillingPeriodName ON BillingPeriod.periodNameId = BillingPeriodName.periodNameId
    WHERE BillingPeriod.periodNameId = ?
      AND Transactionn.deleted = false
      AND BillingPeriod.deleted = false
      AND Tenant.deleted = false
      AND Room.deleted = false
      AND BillingPeriodName.deleted = false
    GROUP BY Transactionn.transactionId, Transactionn.date, Transactionn.amount, Tenant.name, Tenant.tenantId, Tenant.ownContact, Room.roomName, BillingPeriodName.name, BillingPeriod.agreedPrice
    ORDER BY date ASC
  `;

  const params = [periodNameId];
  const unbalanced = await executeQuery(query, params);
  return computeOwingAmounts(unbalanced)

  function computeOwingAmounts(rows) {  
    const cumul = {}; // key: tenantId+period, value: sum so far
    return rows.map(row => {
      const key = row.tenantid
      cumul[key] = (cumul[key] || 0) + row.amount;
      return {
        ...row,
        owingAmount: row.agreedprice - cumul[key]
      };
    })
    .sort((a, b)=> new Date(b.date) - new Date(a.date))
  }
}

async function sendReceipt(transactionId) {
  const detailsQuery = `SELECT 
      t.ownContact,
      tr.periodId,
      bp.agreedPrice,
      tr.amount,
      bpn.name AS periodName,
      bp.ownEndDate
    FROM Transactionn tr
    INNER JOIN BillingPeriod bp ON tr.periodId = bp.periodId
    INNER JOIN Tenant t ON bp.tenantId = t.tenantId
    INNER JOIN BillingPeriodName bpn ON bp.periodNameId = bpn.periodNameId
    WHERE tr.transactionId = ?`;

  const sumQuery = `SELECT 
    SUM(tr.amount) AS totalAmount
    FROM Transactionn tr
    WHERE tr.periodId = ?`;

  try {
    const details = await executeQuery(detailsQuery, [transactionId]);
    if (!details.length) {
      console.log("No details found for transactionId:", transactionId);
      return `No details found for transactionId: ${transactionId}`;
    }

    const periodId = details[0].periodid;
    const sum = await executeQuery(sumQuery, [periodId]);

    function formatNumber(num) {
      return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    let ownContact = details[0].owncontact.trim();

    if (/^0\d{9}$/.test(ownContact)) {
      ownContact = "+256" + ownContact.slice(1);
    } else {
      return `Invalid phone number format: ${ownContact}`;
    }

    const validPrefixes = ["+25677", "+25678", "+25675", "+25670", "+25674", "+25676"];
    if (!validPrefixes.some(prefix => ownContact.startsWith(prefix))) {
      console.log("Invalid phone number format:", ownContact);
      return `Invalid phone number format: ${ownContact}`;
    }

    const sms = AfricasTalking.SMS;
    const options = {
      to: [ownContact],
      // to: ['+256783103587'],
      message: `Hello, We have received your payment of UGX ${formatNumber(details[0].amount)} to Kann Hostel for ${details[0].ownenddate ? `the period ending on ${details[0].ownenddate}` : details[0].periodname}. Your outstanding balance is UGX ${formatNumber(details[0].agreedprice - (sum[0].totalamount || 0))}. Transaction ID: KN${transactionId}. Thank you.`,
      from: 'ATEducTech'
    };

    await sms.send(options)
      .then(response => console.log("SMS sent:", response))
      .catch(error => {
        console.error("Error sending SMS:", error);
        throw error;
      });

    return `Receipt sent to ${ownContact}`;
  } catch (error) {
    console.error("Error in sendReceipt:", error);
    return `Error in sending receipt: ${error}`;
  }
}

async function dashboardTotals(periodNameId) {

  const totals = {
    totalTenants: 0,
    totalFreeSpaces: 0,
    totalPayments: 0,
    totalOutstanding: 0,
    totalMisc: 0,
    totalPastTenants: 0,
  };

  const queries = {
    totalTenants: `
      SELECT COUNT(*) AS totalTenants
      FROM (
        SELECT Tenant.tenantId
        FROM Tenant
        JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
        JOIN Room ON BillingPeriod.roomId = Room.roomId
        LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
        WHERE BillingPeriod.periodNameId = ? 
          AND (BillingPeriod.ownEndDate IS NULL OR BillingPeriod.ownEndDate >= CURRENT_DATE)
          AND Tenant.deleted = false 
          AND BillingPeriod.deleted = false
          AND Room.deleted = false
        GROUP BY Tenant.tenantId
      ) AS groupedRecords;
    `,
    totalFreeSpaces: `
      WITH TotalRoomSpaces AS (
          SELECT COUNT(*) * 2 AS totalSpaces
          FROM Room WHERE Room.deleted = false
      ),
      OccupiedSpaces AS (
          SELECT SUM(CASE WHEN BillingPeriod.periodType = 'single' THEN 2
                  ELSE 1 END) AS occupiedSpaces
          FROM BillingPeriod WHERE periodNameId = ? 
          AND (BillingPeriod.ownEndDate IS NULL OR BillingPeriod.ownEndDate >= CURRENT_DATE)
          AND deleted = false
      )
      SELECT 
          TotalRoomSpaces.totalSpaces - COALESCE(OccupiedSpaces.occupiedSpaces, 0) AS totalFreeSpaces
      FROM TotalRoomSpaces, OccupiedSpaces;
    `,
    totalPayments: `
      SELECT 
        SUM(Transactionn.amount) AS totalPayments
      FROM Transactionn
      JOIN BillingPeriod ON Transactionn.periodId = BillingPeriod.periodId
      WHERE BillingPeriod.periodNameId = ?
        AND Transactionn.deleted = false
        AND BillingPeriod.deleted = false;
    `,
    totalOutstanding: `
        SELECT COALESCE(
            SUM(BillingPeriod.agreedPrice) - (
                SELECT 
                    COALESCE(SUM(Transactionn.amount), 0)  -- Ensure SUM() returns 0 if no transactions exist
                FROM 
                    Transactionn 
                JOIN 
                    BillingPeriod ON Transactionn.periodId = BillingPeriod.periodId 
                WHERE 
                    Transactionn.deleted = false 
                    AND BillingPeriod.deleted = false 
                    AND BillingPeriod.periodNameId = ?
            ), 
            0
        ) AS totalOutstanding
        FROM BillingPeriod
        WHERE 
            BillingPeriod.periodNameId = ? 
            AND BillingPeriod.deleted = false;
    `,
    totalMisc: `
        SELECT 
            COALESCE(SUM(MiscExpense.amount * MiscExpense.quantity), 0) AS totalMisc
        FROM 
            MiscExpense
        WHERE 
            MiscExpense.periodNameId = ?
            AND MiscExpense.deleted = false;
    `,
    totalPastTenants: `
      SELECT COUNT(DISTINCT Tenant.tenantId) AS totalPastTenants
      FROM Tenant
      JOIN BillingPeriod ON Tenant.tenantId = BillingPeriod.tenantId
      JOIN Room ON BillingPeriod.roomId = Room.roomId
      JOIN BillingPeriodName ON BillingPeriod.periodNameId = BillingPeriodName.periodNameId
      LEFT JOIN Transactionn ON BillingPeriod.periodId = Transactionn.periodId 
      WHERE 
      (  
        BillingPeriodName.startingDate < (
          SELECT startingDate 
          FROM BillingPeriodName 
          WHERE periodNameId = ?
        )
        AND Tenant.tenantId NOT IN (
          SELECT tenantId
          FROM BillingPeriod
          WHERE periodNameId = ?
          AND deleted = false
        )    
      ) OR (
        BillingPeriodName.periodNameId = ? 
        AND BillingPeriod.ownEndDate IS NOT NULL 
        AND BillingPeriod.ownEndDate < CURRENT_DATE
      )
      AND Tenant.deleted = false
      AND (Transactionn.deleted = false OR Transactionn.deleted IS NULL)
      AND BillingPeriod.deleted = false
      AND BillingPeriodName.deleted = false
      AND Room.deleted = false;
    `,
  };

  totals.totalTenants = await executeQuery(queries.totalTenants, [periodNameId]);
  totals.totalPayments = await executeQuery(queries.totalPayments, [periodNameId]);
  totals.totalFreeSpaces = await executeQuery(queries.totalFreeSpaces, [periodNameId])
  totals.totalOutstanding = await executeQuery(queries.totalOutstanding, [periodNameId, periodNameId]);
  totals.totalMisc = await executeQuery(queries.totalMisc, [periodNameId]);
  totals.totalPastTenants = await executeQuery(queries.totalPastTenants, [periodNameId, periodNameId, periodNameId]);

  totals.totalTenants = totals.totalTenants[0].totaltenants
  totals.totalPayments = totals.totalPayments[0].totalpayments
  totals.totalFreeSpaces = totals.totalFreeSpaces[0].totalfreespaces
  totals.totalOutstanding = totals.totalOutstanding[0].totaloutstanding
  totals.totalMisc = totals.totalMisc[0].totalmisc
  totals.totalPastTenants = totals.totalPastTenants[0].totalpasttenants

  return totals;
}

function generateRandomRoomName() {
  const letter = String.fromCharCode(65 + Math.floor(Math.random() * 26));
  const number = Math.floor(100 + Math.random() * 900);
  return `${letter}${number}`;
}

async function createDefaultRooms() {
  for (let level = 1; level <= 5; level++) {
    for (let i = 0; i < 40; i++) {
      const roomName = generateRandomRoomName();
      const query = `
        INSERT INTO Room (levelNumber, roomName)
        VALUES (?, ?)
      `;
      const params = [level, roomName];
      try {
        await executeQuery(query, params);
      } catch (error) {
        console.error(`Error creating room ${roomName} on level ${level}:`, error);
      }
    }
  }
}

async function moveMonthlyBillingPeriods(periodNameId) {
  const today = new Date(new Date().getTime() + 3 * 60 * 60 * 1000).toISOString().split('T')[0];

  const query = `UPDATE BillingPeriod SET periodNameId = ? WHERE ownEndDate IS NOT NULL AND ownEndDate >= ?;`

  return executeQuery(query, [periodNameId, today])
}

const query1 = `INSERT INTO Tenant (tenantId, name, gender, age, course, ownContact, nextOfKin, kinContact) VALUES
(1, 'Alice Johnson', 'female', 22, 'Engineering', '1234567001', 'John Johnson', '1234567101'),
(2, 'Bob Smith', 'male', 24, 'Physics', '1234567002', 'Emma Smith', '1234567102'),
(3, 'Carol Brown', 'female', 23, 'Mathematics', '1234567003', 'James Brown', '1234567103'),
(4, 'David Wilson', 'male', 21, 'Chemistry', '1234567004', 'Lily Wilson', '1234567104'),
(5, 'Eva Davis', 'female', 22, 'Biology', '1234567005', 'Michael Davis', '1234567105'),
(6, 'Frank Miller', 'male', 25, 'Architecture', '1234567006', 'Sophia Miller', '1234567106'),
(7, 'Grace Moore', 'female', 20, 'Computer Science', '1234567007', 'Olivia Moore', '1234567107'),
(8, 'Henry Taylor', 'male', 23, 'Business', '1234567008', 'Daniel Taylor', '1234567108'),
(9, 'Ivy Anderson', 'female', 21, 'Art', '1234567009', 'Ella Anderson', '1234567109'),
(10, 'Jack Thomas', 'male', 22, 'History', '1234567010', 'Liam Thomas', '1234567110'),
(11, 'Jane Dyre', 'female', 23, 'Geography', '32322323', 'Neeson', '232323232'),
(12, 'Karl Dyre', 'female', 20, 'CSC', '32322323', 'Karli', '232323232'),
(13, 'Namart Earhe', 'male', 23, 'Theology', '11122323', 'Hope', '237780232');`;

const query2 = `INSERT INTO BillingPeriod (periodId, periodNameId, tenantId, roomId, agreedPrice, periodType, ownStartingDate, ownEndDate) VALUES
(1, 1, 1, 1, 800, 'single', null, null),
(2, 1, 2, 2, 1200, 'double', null, null),
(3, 1, 3, 3, 1000, 'single', null, null),
(4, 2, 4, 6, 1500, 'double', null, null),
(5, 2, 5, 5, 700, 'single', null, null),
(6, 2, 6, 6, 1100, 'double', null, null),
(7, 3, 7, 7, 900, 'single', null, null),
(8, 3, 8, 8, 1300, 'double', null, null),
(9, 3, 9, 9, 600, 'single', null, null),
(10, 3, 10, 10, 1400, 'double', '2025-01-04', '2025-06-24'),
(11, 2, 11, 7, 850, 'single', '2025-01-04', '2025-02-18'),
(12, 1, 12, 8, 1200, 'double', '2024-01-04', '2025-01-04'),
(13, 3, 13, 9, 700, 'single', '2024-06-04', '2025-07-02');`

const query3 = `INSERT INTO Transactionn (periodId, date, amount) VALUES
( 1, '2024-11-01', 400),
( 1, '2024-11-02', 400),
( 2, '2024-11-02', 600),
( 3, '2024-11-03', 500),
( 3, '2024-11-04', 500),
( 4, '2024-11-05', 1000),
( 5, '2024-11-06', 700),
( 6, '2024-11-08', 1100),
( 7, '2024-11-09', 900),
( 10, '2024-11-05', 1400),
( 11, '2024-11-06', 700),
( 12, '2024-11-08', 100),
( 13, '2024-11-09', 600),
( 8, '2024-11-10', 1300);`

// to reset db --but these arent exported for security
// wipeTables() 
// initializeTrigger()
// createDefaultRooms()
// createOtherDefaults()

function createOtherDefaults() {
  setTimeout(async () => { await executeQuery(query1) }, 2000)

  setTimeout(async () => { await executeQuery(query2) }, 3000)

  setTimeout(async () => { await executeQuery(query3) }, 4000)
  for (let i = 1; i <= 10; i++) {
    const ids = [151, 142, 143, 155, 189, 199, 140, 182, 172, 185]
    updateBillingPeriod(i, { roomId: ids[i - 1] })
  }
}

module.exports = {
  createAccount,
  createBillingPeriod,
  createBillingPeriodName,
  // createDefaultRooms,
  createMiscExpense,
  // createOtherDefaults,
  createTenant,
  createTransaction,
  dashboardTotals,
  executeQuery,
  executeQuery,
  getAccountById,
  getAccountsDeadAndLiving,
  getAllRooms,
  getAllTenants,
  getAllTenantsNameAndId,
  getBillingPeriodBeingPaidFor,
  getBillingPeriodById,
  getBillingPeriodNames,
  getFullTenantProfile,
  getLevels,
  getMiscExpenseById,
  getMiscExpensesByDate,
  getMiscExpensesForBillingPeriodName,
  getMonthliesFor,
  getMostRecentTransaction,
  getOlderTenantsThan,
  getOnlyTenantsWithOwingAmt,
  getPotentialTenantRoomsByGender,
  getRoomsAndOccupancyByLevel,
  getTenantsByBillingPeriodName,
  getTenantsByLevel,
  getTenantsAndOwingAmtByRoom,
  getTenantsOfBillingPeriodXButNotY,
  getTenantsPlusOutstandingBalanceAll,
  getTransactionById,
  getTransactions,
  getTransactionsByPeriodNameIdWithMetaData,
  // initializeTrigger,
  login,
  moveMonthlyBillingPeriods,
  searchTenantByName,
  searchTenantNameAndId,
  searchRoomByNamePart,
  sendReceipt,
  updateAccount,
  updateBillingPeriod,
  updateBillingPeriodName,
  updateMiscExpense,
  updateRoom,
  updateTenant,
  updateTransaction,
  // wipeTables
}
