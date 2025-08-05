import fs from 'fs';
import path from 'path';

// === ENHANCED LOGGING SYSTEM ===
const LOG_DIR = "./logs";
const ERROR_LOG_FILE = path.join(LOG_DIR, "error.log");
const ACTIVITY_LOG_FILE = path.join(LOG_DIR, "activity.log");
const POOL_PROCESSING_LOG = path.join(LOG_DIR, "pool_processing.log");
const SWAP_LOG_FILE = path.join(LOG_DIR, "swap.log");
const LIQUIDITY_LOG_FILE = path.join(LOG_DIR, "liquidity.log");
const RECOVERY_LOG_FILE = path.join(LOG_DIR, "recovery.log");

// Create logs directory if it doesn't exist
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  console.log(`📁 Created logs directory: ${LOG_DIR}`);
}

// === LOGGING UTILITIES ===
function getDetailedTimestamp() {
  const now = new Date();
  return now.toLocaleString('id-ID', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3
  });
}

function formatLogEntry(level, category, message, data = null) {
  const timestamp = getDetailedTimestamp();
  const dataStr = data ? ` | Data: ${JSON.stringify(data, null, 0)}` : '';
  return `[${timestamp}] [${level}] [${category}] ${message}${dataStr}\n`;
}

// === ENHANCED LOGGING FUNCTIONS ===
function logError(category, message, error = null, additionalData = null) {
  const timestamp = getDetailedTimestamp();
  const errorDetails = error ? {
    message: error.message,
    stack: error.stack,
    name: error.name
  } : null;
  
  const logData = {
    timestamp,
    category,
    message,
    error: errorDetails,
    additionalData
  };

  // Format for file
  const logEntry = formatLogEntry('ERROR', category, message, logData);
  
  // Write to error log
  fs.appendFileSync(ERROR_LOG_FILE, logEntry);
  
  // Also write to category-specific logs
  const categoryLogFile = path.join(LOG_DIR, `${category.toLowerCase()}_errors.log`);
  fs.appendFileSync(categoryLogFile, logEntry);
  
  // Console output with color
  console.error(`❌ [${category}] ${message}`);
  if (error) {
    console.error(`   Error: ${error.message}`);
  }
  if (additionalData) {
    console.error(`   Data:`, additionalData);
  }
}

function logActivity(category, message, data = null) {
  const logEntry = formatLogEntry('INFO', category, message, data);
  fs.appendFileSync(ACTIVITY_LOG_FILE, logEntry);
  
  // Also write to category-specific activity log
  const categoryLogFile = path.join(LOG_DIR, `${category.toLowerCase()}_activity.log`);
  fs.appendFileSync(categoryLogFile, logEntry);
}

function logWarning(category, message, data = null) {
  const logEntry = formatLogEntry('WARN', category, message, data);
  fs.appendFileSync(ACTIVITY_LOG_FILE, logEntry);
  
  console.warn(`⚠️ [${category}] ${message}`);
  if (data) {
    console.warn(`   Data:`, data);
  }
}

function logSuccess(category, message, data = null) {
  const logEntry = formatLogEntry('SUCCESS', category, message, data);
  fs.appendFileSync(ACTIVITY_LOG_FILE, logEntry);
  
  console.log(`✅ [${category}] ${message}`);
  if (data) {
    console.log(`   Data:`, data);
  }
}

// === SPECIALIZED LOGGING FUNCTIONS ===
function logPoolProcessing(action, poolSymbol, poolAddress, status, data = null) {
  const logData = {
    action,
    poolSymbol,
    poolAddress,
    status,
    timestamp: getDetailedTimestamp(),
    data
  };
  
  const logEntry = formatLogEntry('POOL', action, `${poolSymbol} (${poolAddress.slice(0, 8)}...) - ${status}`, logData);
  fs.appendFileSync(POOL_PROCESSING_LOG, logEntry);
  
  logActivity('POOL', `${action}: ${poolSymbol} - ${status}`, logData);
}

function logSwapOperation(poolSymbol, fromToken, toToken, amount, status, signature = null, error = null, dexName = null) {
  const logData = {
    pool: poolSymbol,
    from: fromToken,
    to: toToken,
    amount: amount,
    status: status,
    timestamp: getCurrentTimestamp()
  };

  if (dexName) {
    logData.dex = dexName;
  }

  if (signature) {
    logData.signature = signature;
  }

  if (error) {
    logData.error = error.message;
  }

  switch (status) {
    case 'SUCCESS':
      logSuccess('SWAP_OP', `${fromToken} → ${toToken} swap successful${dexName ? ` (${dexName})` : ''}`, logData);
      // Enhanced console output for successful swaps
      console.log(`  💱 DEX Swap: ${amount} ${fromToken} → ${toToken}`);
      if (dexName) {
        console.log(`  🏪 DEX: ${dexName}`);
      }
      if (signature) {
        console.log(`  📜 Signature: ${signature.slice(0, 12)}...`);
      }
      break;
    case 'FAILED':
      logError('SWAP_OP', `${fromToken} → ${toToken} swap failed${dexName ? ` (${dexName})` : ''}`, error, logData);
      break;
    case 'SKIPPED':
      logWarning('SWAP_OP', `${fromToken} → ${toToken} swap skipped${dexName ? ` (${dexName})` : ''}`, logData);
      break;
    default:
      logActivity('SWAP_OP', `${fromToken} → ${toToken} swap ${status}${dexName ? ` (${dexName})` : ''}`, logData);
  }
}

function logLiquidityOperation(poolSymbol, poolAddress, action, status, data = null, error = null) {
  const logData = {
    poolSymbol,
    poolAddress,
    action,
    status,
    timestamp: getDetailedTimestamp(),
    data
  };

  if (status === 'SUCCESS') {
    const logEntry = formatLogEntry('LIQUIDITY', 'SUCCESS', `${action} for ${poolSymbol}`, logData);
    fs.appendFileSync(LIQUIDITY_LOG_FILE, logEntry);
    logSuccess('LIQUIDITY', `${action} for ${poolSymbol}`, data);
  } else if (status === 'FAILED') {
    const logEntry = formatLogEntry('LIQUIDITY', 'FAILED', `${action} for ${poolSymbol}`, { ...logData, error: error?.message });
    fs.appendFileSync(LIQUIDITY_LOG_FILE, logEntry);
    logError('LIQUIDITY', `${action} for ${poolSymbol}`, error, logData);
  }
}

function logRecoveryOperation(poolSymbol, tokenMint, status, signature = null, error = null, balanceInfo = null) {
  const logData = {
    poolSymbol,
    tokenMint,
    status,
    signature,
    balanceInfo,
    timestamp: getDetailedTimestamp()
  };

  if (status === 'SUCCESS') {
    const logEntry = formatLogEntry('RECOVERY', 'SUCCESS', `Recovered ${poolSymbol} → SOL`, logData);
    fs.appendFileSync(RECOVERY_LOG_FILE, logEntry);
    logSuccess('RECOVERY', `Recovered ${poolSymbol} → SOL`, { signature: signature?.slice(0, 8) + '...', balanceInfo });
  } else if (status === 'FAILED') {
    const logEntry = formatLogEntry('RECOVERY', 'FAILED', `Failed to recover ${poolSymbol}`, { ...logData, error: error?.message });
    fs.appendFileSync(RECOVERY_LOG_FILE, logEntry);
    logError('RECOVERY', `Failed to recover ${poolSymbol}`, error, logData);
  } else if (status === 'STUCK') {
    const logEntry = formatLogEntry('RECOVERY', 'STUCK', `Tokens stuck: ${poolSymbol}`, logData);
    fs.appendFileSync(RECOVERY_LOG_FILE, logEntry);
    logError('RECOVERY', `Tokens stuck: ${poolSymbol}`, null, logData);
  }
}

// === ENHANCED ERROR WRAPPER FUNCTIONS ===
async function executeWithLogging(operation, category, description, additionalData = null) {
  const startTime = Date.now();
  try {
    logActivity(category, `Starting: ${description}`, additionalData);
    const result = await operation();
    const duration = Date.now() - startTime;
    logSuccess(category, `Completed: ${description} (${duration}ms)`, { result, duration });
    return result;
  } catch (error) {
    const duration = Date.now() - startTime;
    logError(category, `Failed: ${description} (${duration}ms)`, error, additionalData);
    throw error;
  }
}

// === LOG ANALYSIS UTILITIES ===
function getLogStats() {
  const stats = {
    timestamp: getDetailedTimestamp(),
    files: {}
  };

  const logFiles = [
    { name: 'error.log', path: ERROR_LOG_FILE },
    { name: 'activity.log', path: ACTIVITY_LOG_FILE },
    { name: 'pool_processing.log', path: POOL_PROCESSING_LOG },
    { name: 'swap.log', path: SWAP_LOG_FILE },
    { name: 'liquidity.log', path: LIQUIDITY_LOG_FILE },
    { name: 'recovery.log', path: RECOVERY_LOG_FILE }
  ];

  logFiles.forEach(({ name, path: filePath }) => {
    if (fs.existsSync(filePath)) {
      const stat = fs.statSync(filePath);
      const content = fs.readFileSync(filePath, 'utf8');
      const lines = content.split('\n').filter(line => line.trim());
      
      stats.files[name] = {
        size: stat.size,
        sizeHuman: (stat.size / 1024).toFixed(2) + ' KB',
        lines: lines.length,
        lastModified: stat.mtime,
        exists: true
      };
    } else {
      stats.files[name] = { exists: false };
    }
  });

  return stats;
}

// Tambahkan function ini di logging.js
function getCurrentTimestamp() {
  return new Date().toLocaleString('id-ID', {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

// Update function displayLogStats menjadi:
function displayLogStats() {
  const stats = getLogStats();
  console.log(`\n📊 === LOG STATISTICS [${getCurrentTimestamp()}] ===`); // ✅ Gunakan getCurrentTimestamp()
  
  Object.entries(stats.files).forEach(([filename, info]) => {
    if (info.exists) {
      console.log(`📄 ${filename}: ${info.lines} lines, ${info.sizeHuman}`);
    } else {
      console.log(`📄 ${filename}: Not created yet`);
    }
  });
}

// === LOG ROTATION (to prevent huge files) ===
function rotateLogs() {
  const MAX_LOG_SIZE = 50 * 1024 * 1024; // 50MB
  const logFiles = [ERROR_LOG_FILE, ACTIVITY_LOG_FILE, POOL_PROCESSING_LOG, SWAP_LOG_FILE, LIQUIDITY_LOG_FILE, RECOVERY_LOG_FILE];

  logFiles.forEach(logFile => {
    if (fs.existsSync(logFile)) {
      const stats = fs.statSync(logFile);
      if (stats.size > MAX_LOG_SIZE) {
        const backupFile = logFile.replace('.log', `_${Date.now()}.log`);
        fs.renameSync(logFile, backupFile);
        console.log(`📁 Rotated log: ${path.basename(logFile)} → ${path.basename(backupFile)}`);
        logActivity('SYSTEM', `Log rotated: ${path.basename(logFile)}`, { oldSize: stats.size, backupFile });
      }
    }
  });
}

// === ENHANCED RETRY UTILITY WITH LOGGING ===
async function retryWithDetailedLogging(operation, maxRetries, delayMs, operationName, category, additionalData = null, preRetryCheck = null) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      logActivity(category, `${operationName} - Attempt ${attempt}/${maxRetries}`, { ...additionalData, attempt });
      const result = await operation();
      
      if (attempt > 1) {
        logSuccess(category, `${operationName} succeeded on attempt ${attempt}`, { attempt, result });
      } else {
        logSuccess(category, `${operationName} succeeded`, { result });
      }
      
      return result;
    } catch (error) {
      const retryData = { ...additionalData, attempt, maxRetries, error: error.message };
      
      if (attempt === maxRetries) {
        logError(category, `${operationName} failed after ${maxRetries} attempts`, error, retryData);
        throw new Error(`${operationName} failed after ${maxRetries} attempts: ${error.message}`);
      }
      
      logWarning(category, `${operationName} failed on attempt ${attempt}`, retryData);
      
      // Check if we should skip retry
      if (preRetryCheck) {
        try {
          const shouldSkipRetry = await preRetryCheck();
          if (shouldSkipRetry) {
            logSuccess(category, `${operationName} actually succeeded, skipping retry`, { attempt });
            return "verified_success";
          }
        } catch (checkError) {
          logWarning(category, `Pre-retry check failed for ${operationName}`, { checkError: checkError.message });
        }
      }
      
      logActivity(category, `Waiting ${delayMs}ms before retry`, { attempt, nextAttempt: attempt + 1 });
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
}

// === SYSTEM MONITORING ===
function startLogMonitoring() {
  // Log rotation every hour
  setInterval(rotateLogs, 60 * 60 * 1000);
  
  // Log stats every 10 minutes
  setInterval(displayLogStats, 10 * 60 * 1000);
  
  console.log("📊 Log monitoring started");
  logActivity('SYSTEM', 'Log monitoring started', { 
    logDir: LOG_DIR,
    rotationInterval: '1 hour',
    statsInterval: '10 minutes'
  });
}

// === EXPORT LOGGING FUNCTIONS ===
export {
  logError,
  logActivity,
  logWarning,
  logSuccess,
  logPoolProcessing,
  logSwapOperation,
  logLiquidityOperation,
  logRecoveryOperation,
  executeWithLogging,
  retryWithDetailedLogging,
  getLogStats,
  displayLogStats,
  startLogMonitoring,
  getDetailedTimestamp,
  getCurrentTimestamp,
  rotateLogs
};
