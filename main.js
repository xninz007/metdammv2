import fs from 'fs';
import path from 'path';
import {
  Connection,
  PublicKey,
  Keypair,
  sendAndConfirmTransaction,
  SystemProgram,
} from "@solana/web3.js";
import {
  TOKEN_PROGRAM_ID,
  ASSOCIATED_TOKEN_PROGRAM_ID,
  getAssociatedTokenAddressSync,
  getMint,
} from "@solana/spl-token";

// === CORRECTED IMPORTS (Only what's actually used) ===
import {
  logError,
  logActivity,
  logWarning,
  logSuccess,
  logPoolProcessing,
  logSwapOperation,
  logLiquidityOperation,
  retryWithDetailedLogging,
  getDetailedTimestamp,
  displayLogStats,
  getCurrentTimestamp,
  startLogMonitoring
} from './logging.js';

import { CpAmm, getUnClaimReward } from "@meteora-ag/cp-amm-sdk";
import { BN } from "bn.js";
import bs58 from "bs58";
import axios from "axios";
import { VersionedTransaction } from "@solana/web3.js";

// === KONFIGURASI ===
const RPC_URL = "";
const RPC_URL_2 = "";
const PRIVATE_KEY_BS58 = ""; // 🛑 KOSONGKAN UNTUK KEAMANAN - ISI SAAT RUNNING
const SENT_POOLS_FILE = "./sent_pools.json";

// === KONFIGURASI MONITORING ===
const POOL_CHECK_INTERVAL = 5 * 1000;
const POSITION_DISPLAY_INTERVAL = 60 * 1000;

// === KONFIGURASI AUTOSWAP ===
const SOL_MINT = "So11111111111111111111111111111111111111112";
const DEX_SWAP_AMOUNTS = {
  // DEX names dan swap amount dalam SOL
  "pumpfun-amm": 0.05,
  "raydium-clmm": 0.03,
  "raydium-cp-swaps": 0.03,
  "jup-studio": 0.03,
  "meteora-damm-v2": 0.03,
  "pumpfun": 0.01,
  "moonit-cp": 0.01,
  "moonshot": 0.01,
  "default": 0.025              // Default amount jika DEX tidak dikenal
};

// === KONFIGURASI DAMM BOT ===
const USE_MAX_BALANCE = true;
const MAX_BALANCE_PERCENTAGE = 100;
const IS_TOKEN_A = true;
const SLIPPAGE = 5;

// === KONFIGURASI RETRY ===
const MAX_SWAP_RETRIES = 3;
const MAX_LIQUIDITY_RETRIES = 7;
const MAX_RECOVERY_RETRIES = 2;
const RETRY_DELAY = 3000;
const MAX_POOL_RETRIES = 1;

// === INISIALISASI ===
const connection = new Connection(RPC_URL, "confirmed");
const connection2 = new Connection(RPC_URL_2, "confirmed");
const cpAmm = new CpAmm(connection);
const cpAmm2 = new CpAmm(connection2);

// Check private key before creating wallet
if (!PRIVATE_KEY_BS58) {
  console.error("❌ PRIVATE_KEY_BS58 harus diisi!");
  console.error("   Silakan isi private key Anda di konfigurasi");
  process.exit(1);
}

const wallet = Keypair.fromSecretKey(bs58.decode(PRIVATE_KEY_BS58));
const DAMM_PROGRAM_ID = new PublicKey("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG");

// === ENHANCED GLOBAL STATE ===
const tokenMetadataCache = new Map();
const poolProcessingState = new Map();
const poolRetryCount = new Map();
let lastFileModified = 0;
let positionCount = 0;
let poolsData = [];
let symbol_global = new Map();
let poolQueue = [];
let isProcessingPool = false;
let initialPoolSnapshot = new Set();
let snapshotCreated = false;

// === UTILITY FUNCTIONS ===

function derivePositionNftAccount(positionNftMint) {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("position_nft_account"), positionNftMint.toBuffer()],
    DAMM_PROGRAM_ID
  )[0];
}

// === FUNGSI UNTUK MENDAPATKAN SWAP AMOUNT BERDASARKAN DEX ===
function getSwapAmountForDex(dexName) {
  if (!dexName || typeof dexName !== 'string') {
    logWarning('DEX_CONFIG', 'Invalid DEX name provided, using default amount', {
      providedDex: dexName,
      defaultAmount: DEX_SWAP_AMOUNTS.default
    });
    return {
      amountSOL: DEX_SWAP_AMOUNTS.default,
      amountLamports: DEX_SWAP_AMOUNTS.default * 1e9,
      source: 'default_fallback'
    };
  }

  // Normalize DEX name (lowercase, trim spaces)
  const normalizedDex = dexName.toLowerCase().trim();
  
  // Check if we have a specific configuration for this DEX
  const swapAmountSOL = DEX_SWAP_AMOUNTS[normalizedDex] || DEX_SWAP_AMOUNTS.default;
  const isCustomAmount = DEX_SWAP_AMOUNTS.hasOwnProperty(normalizedDex);
  
  const result = {
    amountSOL: swapAmountSOL,
    amountLamports: swapAmountSOL * 1e9,
    source: isCustomAmount ? 'dex_specific' : 'default'
  };

  logActivity('DEX_CONFIG', `Swap amount determined for ${dexName}`, {
    originalDex: dexName,
    normalizedDex: normalizedDex,
    amountSOL: swapAmountSOL,
    amountLamports: result.amountLamports,
    configSource: result.source,
    isCustomAmount
  });

  return result;
}

// === FUNGSI UNTUK MENDISPLAY KONFIGURASI DEX ===
function displayDexConfiguration() {
  console.log("\n💰 === DEX SWAP AMOUNT CONFIGURATION ===");
  
  const sortedDexes = Object.entries(DEX_SWAP_AMOUNTS)
    .filter(([dex]) => dex !== 'default')
    .sort(([,a], [,b]) => b - a); // Sort by amount descending
  
  sortedDexes.forEach(([dex, amount]) => {
    const riskLevel = amount <= 0.005 ? '🔴 HIGH' : amount <= 0.01 ? '🟡 MED' : '🟢 LOW';
    console.log(`  ${dex.padEnd(20)} : ${amount.toString().padEnd(6)} SOL ${riskLevel}`);
  });
  
  console.log(`  ${'default'.padEnd(20)} : ${DEX_SWAP_AMOUNTS.default.toString().padEnd(6)} SOL 🔵 FALLBACK`);
  console.log(`📊 Total DEX configurations: ${sortedDexes.length + 1}`);
  
  // Calculate total if all DEXes used once
  const totalAmount = Object.values(DEX_SWAP_AMOUNTS).reduce((sum, amount) => sum + amount, 0);
  console.log(`💎 Total if all DEXes used: ${totalAmount.toFixed(3)} SOL`);
  console.log(`🎯 Risk distribution: High-risk DEXes use smaller amounts`);
}

// === BALANCE VERIFICATION FUNCTIONS ===
async function getTokenBalance(mintAddress, useRPC2 = true) {
  try {
    const tokenAccount = getAssociatedTokenAddressSync(
      new PublicKey(mintAddress),
      wallet.publicKey
    );

    const connectionToUse = useRPC2 ? connection2 : connection;
    const balance = await connectionToUse.getTokenAccountBalance(tokenAccount);
    return new BN(balance.value.amount);
  } catch (error) {
    return new BN(0);
  }
}

async function hasTokenBalance(mintAddress, useRPC2 = true) {
  const balance = await getTokenBalance(mintAddress, useRPC2);
  return !balance.isZero();
}

// === SYMBOL GLOBAL FUNCTIONS ===
function updateSymbolGlobal() {
  symbol_global.clear();

  poolsData.forEach(pool => {
    if (pool.mint && pool.symbol) {
      symbol_global.set(pool.mint, pool.symbol);
    }
  });

  console.log(`🔄 Updated symbol_global with ${symbol_global.size} mappings`);

  if (symbol_global.size > 0) {
    console.log("📋 Symbol mappings:");
    let count = 0;
    for (const [mint, symbol] of symbol_global.entries()) {
      console.log(`  ${mint.slice(0, 8)}... -> ${symbol}`);
      count++;
      if (count >= 3) break;
    }
    if (symbol_global.size > 3) {
      console.log(`  ... and ${symbol_global.size - 3} more`);
    }
  }
}

// === FIXED getTokenMetadata dengan Mint-Based Fallback ===
async function getTokenMetadata(mintAddress, poolAddress = null) {
  const mintKey = mintAddress.toBase58();

  if (tokenMetadataCache.has(mintKey)) {
    return tokenMetadataCache.get(mintKey);
  }

  try {
    // Try symbol_global first
    if (symbol_global.has(mintKey)) {
      const symbol = symbol_global.get(mintKey);
      const mintInfo = await getMint(connection, mintAddress);
      const metadata = {
        mint: mintAddress,
        decimals: mintInfo.decimals,
        symbol: symbol,
        name: symbol,
        supply: mintInfo.supply
      };
      tokenMetadataCache.set(mintKey, metadata);
      return metadata;
    }

    // Try pool data lookup
    if (poolAddress && poolsData.length > 0) {
      const poolKey = poolAddress.toBase58();

      const poolByAddress = poolsData.find(pool => pool.pool_address === poolKey);
      if (poolByAddress && poolByAddress.mint === mintKey) {
        const mintInfo = await getMint(connection, mintAddress);
        const symbol = poolByAddress.symbol || `TOKEN_${mintKey.slice(0, 8)}`;
        const metadata = {
          mint: mintAddress,
          decimals: mintInfo.decimals,
          symbol: symbol,
          name: symbol,
          supply: mintInfo.supply
        };
        tokenMetadataCache.set(mintKey, metadata);
        return metadata;
      }

      const poolByMint = poolsData.find(pool => pool.mint === mintKey);
      if (poolByMint) {
        const mintInfo = await getMint(connection, mintAddress);
        const symbol = poolByMint.symbol || `TOKEN_${mintKey.slice(0, 8)}`;
        const metadata = {
          mint: mintAddress,
          decimals: mintInfo.decimals,
          symbol: symbol,
          name: symbol,
          supply: mintInfo.supply
        };
        tokenMetadataCache.set(mintKey, metadata);
        return metadata;
      }

      if (poolByAddress && mintKey !== "So11111111111111111111111111111111111111112") {
        const mintInfo = await getMint(connection, mintAddress);
        const symbol = poolByAddress.symbol || `TOKEN_${mintKey.slice(0, 8)}`;
        const metadata = {
          mint: mintAddress,
          decimals: mintInfo.decimals,
          symbol: symbol,
          name: symbol,
          supply: mintInfo.supply
        };
        tokenMetadataCache.set(mintKey, metadata);
        return metadata;
      }
    }

    // ✅ CRITICAL FIX: Always use mint-based symbol instead of "UNKNOWN"
    const mintInfo = await getMint(connection, mintAddress);
    let symbol = `TOKEN_${mintKey.slice(0, 8)}`;
    let name = `Token ${mintKey.slice(0, 8)}`;

    const knownTokens = {
      "So11111111111111111111111111111111111111112": { symbol: "SOL", name: "Solana" },
      "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": { symbol: "USDC", name: "USD Coin" },
      "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": { symbol: "USDT", name: "Tether USD" },
      "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": { symbol: "mSOL", name: "Marinade SOL" },
      "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj": { symbol: "stSOL", name: "Lido Staked SOL" },
      "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": { symbol: "BONK", name: "Bonk" },
      "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": { symbol: "JUP", name: "Jupiter" },
    };

    if (knownTokens[mintKey]) {
      symbol = knownTokens[mintKey].symbol;
      name = knownTokens[mintKey].name;
    }

    const metadata = {
      mint: mintAddress,
      decimals: mintInfo.decimals,
      symbol,
      name,
      supply: mintInfo.supply
    };

    tokenMetadataCache.set(mintKey, metadata);
    
    // ✅ Also update symbol_global for future reference
    if (!symbol_global.has(mintKey)) {
      symbol_global.set(mintKey, symbol);
      console.log(`🔄 Auto-added to symbol_global: ${mintKey} → ${symbol}`);
    }
    
    return metadata;

  } catch (error) {
    // ✅ CRITICAL FIX: Never return "UNKNOWN", always use mint-based
    const fallbackSymbol = `TOKEN_${mintKey.slice(0, 8)}`;
    const fallbackMetadata = {
      mint: mintAddress,
      decimals: 9, // Safe fallback
      symbol: fallbackSymbol,
      name: `Token ${mintKey.slice(0, 8)}`,
      supply: new BN(0)
    };

    tokenMetadataCache.set(mintKey, fallbackMetadata);
    
    // ✅ Also update symbol_global
    if (!symbol_global.has(mintKey)) {
      symbol_global.set(mintKey, fallbackSymbol);
      console.log(`🔄 Auto-added fallback to symbol_global: ${mintKey} → ${fallbackSymbol}`);
    }
    
    console.log(`⚠️ getTokenMetadata error for ${mintKey}, using fallback: ${fallbackSymbol}`);
    return fallbackMetadata;
  }
}
function humanAmountToTokenAmount(humanAmount, decimals) {
  return new BN(humanAmount * Math.pow(10, decimals));
}

function tokenAmountToHuman(tokenAmount, decimals) {
  return parseFloat(tokenAmount.toString()) / Math.pow(10, decimals);
}

// === FIX 2: Enhanced Recovery Function ===
async function swapBackToSOL(tokenMint, poolSymbol) {
  // ✅ Handle null/undefined poolSymbol
  let safePoolSymbol = poolSymbol;
  
  if (!safePoolSymbol || safePoolSymbol === 'null' || safePoolSymbol === 'undefined') {
    safePoolSymbol = `TOKEN_${tokenMint.slice(0, 8)}`;
    console.log(`🔧 Recovery: Using fallback symbol ${safePoolSymbol} for ${tokenMint}`);
  }
  
  logActivity('RECOVERY', `Starting recovery swap: ${safePoolSymbol} → SOL`, {
    tokenMint,
    poolSymbol: safePoolSymbol,
    originalSymbol: poolSymbol,
    wasNull: !poolSymbol
  });

  const tokenBalance = await getTokenBalance(tokenMint, true);
  if (tokenBalance.isZero()) {
    logWarning('RECOVERY', `No ${safePoolSymbol} balance to swap back`, {
      tokenMint,
      poolSymbol: safePoolSymbol
    });
    return null;
  }

  // ✅ Use proper decimals instead of hardcoded 9
  let tokenDecimals = 9;
  try {
    const mintInfo = await getMint(connection, new PublicKey(tokenMint));
    tokenDecimals = mintInfo.decimals;
  } catch (error) {
    console.log(`⚠️ Could not get decimals for ${tokenMint}, using 9`);
  }

  const tokenBalanceHuman = tokenAmountToHuman(tokenBalance, tokenDecimals);
  logActivity('RECOVERY', `Attempting to swap ${tokenBalanceHuman} ${safePoolSymbol} → SOL`, {
    tokenMint,
    balance: tokenBalanceHuman,
    balanceRaw: tokenBalance.toString(),
    decimals: tokenDecimals
  });

  const recoverySwapOperation = async () => {
    logActivity('RECOVERY', `Executing recovery swap: ${safePoolSymbol} → SOL`);

    const currentBalance = await getTokenBalance(tokenMint, true);
    if (currentBalance.isZero()) {
      throw new Error("Token balance is zero, cannot proceed with recovery");
    }

    const taker = wallet.publicKey.toBase58();
    const orderUrl = `https://ultra-api.jup.ag/order?inputMint=${tokenMint}&outputMint=${SOL_MINT}&amount=${currentBalance.toString()}&taker=${taker}&swapMode=ExactIn`;

    const { data: order } = await axios.get(orderUrl);
    if (!order?.transaction || !order?.requestId) {
      throw new Error("Recovery swap order response tidak valid");
    }

    logActivity('RECOVERY', `Jupiter recovery order created for ${safePoolSymbol}`, {
      requestId: order.requestId,
      amount: tokenAmountToHuman(currentBalance, tokenDecimals)
    });

    const txBuffer = Buffer.from(order.transaction, "base64");
    const tx = VersionedTransaction.deserialize(txBuffer);
    tx.sign([wallet]);

    const { data: result } = await axios.post("https://ultra-api.jup.ag/execute", {
      signedTransaction: Buffer.from(tx.serialize()).toString("base64"),
      requestId: order.requestId,
    });

    if (result.status !== "Success") {
      throw new Error(`Recovery swap gagal: ${JSON.stringify(result)}`);
    }

    logSuccess('RECOVERY', `Recovery swap successful: ${safePoolSymbol} → SOL`, {
      signature: result.signature,
      poolSymbol: safePoolSymbol
    });

    return result.signature;
  };

  const recoveryRetryCheck = async () => {
    logActivity('RECOVERY', `Verifying recovery swap for ${safePoolSymbol}`);
    await new Promise(resolve => setTimeout(resolve, 2000));

    const balanceAfter = await getTokenBalance(tokenMint, true);
    const swapSucceeded = balanceAfter.lt(tokenBalance);

    if (swapSucceeded) {
      logSuccess('RECOVERY', `Recovery verified: ${safePoolSymbol} balance decreased`, {
        balanceBefore: tokenAmountToHuman(tokenBalance, tokenDecimals),
        balanceAfter: tokenAmountToHuman(balanceAfter, tokenDecimals)
      });
    } else {
      logWarning('RECOVERY', `Recovery verification failed: ${safePoolSymbol} balance unchanged`, {
        balanceBefore: tokenAmountToHuman(tokenBalance, tokenDecimals),
        balanceAfter: tokenAmountToHuman(balanceAfter, tokenDecimals)
      });
    }

    return swapSucceeded;
  };

  try {
    const signature = await retryWithDetailedLogging(
      recoverySwapOperation,
      MAX_RECOVERY_RETRIES,
      RETRY_DELAY,
      `Recovery Swap (${safePoolSymbol} → SOL)`,
      'RECOVERY',
      {
        tokenMint,
        poolSymbol: safePoolSymbol,
        initialBalance: tokenBalanceHuman
      },
      recoveryRetryCheck
    );

    if (signature && signature !== "verified_success") {
      logSuccess('RECOVERY', `Recovery completed: ${safePoolSymbol} → SOL`, {
        signature,
        recoveredAmount: tokenBalanceHuman,
        tokenMint
      });
      return signature;
    } else if (signature === "verified_success") {
      logSuccess('RECOVERY', `Recovery verified successful: ${safePoolSymbol} → SOL`, {
        note: 'Balance verification confirmed success',
        recoveredAmount: tokenBalanceHuman
      });
      return "verified_success";
    }

    return null;

  } catch (error) {
    const finalBalance = await getTokenBalance(tokenMint, true);
    const finalBalanceHuman = tokenAmountToHuman(finalBalance, tokenDecimals);

    logError('RECOVERY', `Recovery failed: ${safePoolSymbol} tokens may be stuck`, error, {
      poolSymbol: safePoolSymbol,
      tokenMint,
      stuckAmount: finalBalanceHuman,
      note: 'Manual intervention needed'
    });

    console.log(`  ⚠️ ${safePoolSymbol} tokens may remain in wallet - manual intervention needed`);
    console.log(`  📊 Stuck tokens: ${finalBalanceHuman} ${safePoolSymbol} (${tokenMint})`);

    return null;
  }
}

async function autoSwap({ inputMint, outputMint, poolData, signer }) {
  // Extract DEX info from poolData (NEW: poolData parameter instead of amountInLamports)
  const dexName = poolData?.dex || 'unknown';
  const poolSymbol = poolData?.symbol || symbol_global.get(outputMint) || outputMint.slice(0, 8) + '...';
  
  // Get DEX-specific swap amount
  const swapConfig = getSwapAmountForDex(dexName);

  if (!signer || !signer.publicKey) {
    const error = new Error("Parameter 'signer' (Keypair) harus disediakan");
    logError('SWAP', 'Invalid signer parameter', error, { 
      inputMint, 
      outputMint, 
      dexName,
      poolSymbol 
    });
    throw error;
  }

  logActivity('SWAP', `Starting DEX-specific swap: SOL → ${poolSymbol}`, {
    inputMint, 
    outputMint, 
    dexName,
    swapAmountSOL: swapConfig.amountSOL,
    swapAmountLamports: swapConfig.amountLamports,
    configSource: swapConfig.source,
    signer: signer.publicKey.toBase58()
  });

  const preSwapCheck = async () => {
    logActivity('SWAP', `Checking existing ${poolSymbol} balance`, { 
      outputMint,
      dexName 
    });
    const hasBalance = await hasTokenBalance(outputMint, true);
    if (hasBalance) {
      const balance = await getTokenBalance(outputMint, true);
      logWarning('SWAP', `Already have ${poolSymbol} tokens, skipping swap`, {
        existingBalance: balance.toString(), 
        poolSymbol,
        dexName,
        savedAmount: swapConfig.amountSOL + ' SOL'
      });
      return true;
    }
    logActivity('SWAP', `No existing ${poolSymbol} balance, proceeding with DEX-specific swap`, {
      dexName,
      amount: swapConfig.amountSOL + ' SOL'
    });
    return false;
  };

  const shouldSkip = await preSwapCheck();
  if (shouldSkip) {
    logSwapOperation(poolSymbol, 'SOL', poolSymbol, swapConfig.amountSOL, 'SKIPPED', null, null, dexName);
    return "swap_already_done";
  }

  const swapOperation = async () => {
    logActivity('SWAP', `Executing DEX-specific swap: ${swapConfig.amountSOL} SOL → ${poolSymbol}`, { 
      outputMint,
      dexName,
      configSource: swapConfig.source
    });

    const taker = signer.publicKey.toBase58();
    const orderUrl = `https://ultra-api.jup.ag/order?inputMint=${inputMint}&outputMint=${outputMint}&amount=${swapConfig.amountLamports}&taker=${taker}&swapMode=ExactIn`;

    try {
      const { data: order } = await axios.get(orderUrl);
      if (!order?.transaction || !order?.requestId) {
        throw new Error("Order response tidak valid");
      }

      logActivity('SWAP', `Jupiter order created for ${poolSymbol} (${dexName})`, {
        requestId: order.requestId,
        dexName,
        amount: swapConfig.amountSOL + ' SOL',
        orderUrl: orderUrl.substring(0, 100) + '...'
      });

      const txBuffer = Buffer.from(order.transaction, "base64");
      const tx = VersionedTransaction.deserialize(txBuffer);
      tx.sign([signer]);

      const { data: result } = await axios.post("https://ultra-api.jup.ag/execute", {
        signedTransaction: Buffer.from(tx.serialize()).toString("base64"),
        requestId: order.requestId,
      });

      if (result.status !== "Success") {
        throw new Error(`Swap gagal: ${JSON.stringify(result)}`);
      }

      logSwapOperation(poolSymbol, 'SOL', poolSymbol, swapConfig.amountSOL, 'SUCCESS', result.signature, null, dexName);
      return result.signature;

    } catch (error) {
      logSwapOperation(poolSymbol, 'SOL', poolSymbol, swapConfig.amountSOL, 'FAILED', null, error, dexName);
      throw error;
    }
  };

  const swapRetryCheck = async () => {
    logActivity('SWAP', `Verifying if previous DEX-specific swap actually succeeded for ${poolSymbol}`, {
      dexName
    });
    await new Promise(resolve => setTimeout(resolve, 2000));
    const hasBalance = await hasTokenBalance(outputMint, true);

    if (hasBalance) {
      logSuccess('SWAP', `DEX-specific swap verification successful for ${poolSymbol}`, { 
        hasBalance,
        dexName,
        amount: swapConfig.amountSOL + ' SOL'
      });
    } else {
      logWarning('SWAP', `DEX-specific swap verification failed for ${poolSymbol}`, { 
        hasBalance,
        dexName
      });
    }

    return hasBalance;
  };

  try {
    return await retryWithDetailedLogging(
      swapOperation,
      MAX_SWAP_RETRIES,
      RETRY_DELAY,
      `AutoSwap DEX-Specific (SOL → ${poolSymbol} via ${dexName})`,
      'SWAP',
      { 
        inputMint, 
        outputMint, 
        amountSOL: swapConfig.amountSOL,
        dexName,
        configSource: swapConfig.source
      },
      swapRetryCheck
    );
  } catch (error) {
    logError('SWAP', `DEX-specific AutoSwap final failure for ${poolSymbol}`, error, {
      inputMint, 
      outputMint, 
      amountSOL: swapConfig.amountSOL,
      dexName,
      maxRetries: MAX_SWAP_RETRIES,
      suggestion: `Consider adjusting swap amount for ${dexName} DEX`
    });
    throw error;
  }
}

// === SLIPPAGE HELPER FUNCTIONS ===
function getMaxAmountWithSlippage(amount, rate) {
  // Formula: amount * (100 + rate) / 100
  const hundredBN = new BN(100);
  const rateBN = new BN(Math.floor(rate * 100)); // Convert to basis points
  const multiplier = hundredBN.add(rateBN);
  return amount.mul(multiplier).div(hundredBN);
}

function getMinAmountWithSlippage(amount, rate) {
  // Formula: amount * (100 - rate) / 100
  const hundredBN = new BN(100);
  const rateBN = new BN(Math.floor(rate * 100)); // Convert to basis points
  const multiplier = hundredBN.sub(rateBN);
  return amount.mul(multiplier).div(hundredBN);
}

function getPriceImpact(actualAmount, idealAmount) {
  // Formula: ((idealAmount - actualAmount) / idealAmount) * 100
  if (idealAmount.isZero()) return 0;
  
  const difference = idealAmount.sub(actualAmount);
  const impact = difference.mul(new BN(10000)).div(idealAmount);
  return parseFloat(impact.toString()) / 100; // Convert back to percentage
}

// === OPTIMIZED addSingleSidedLiquidity dengan getUserPositionByPool ===
async function addSingleSidedLiquidity(poolAddress) {
  const poolAddressStr = poolAddress.toString();

  // ✅ WRAP ENTIRE FUNCTION dengan retry
  const liquidityOperationWithRetry = async () => {
    logActivity('LIQUIDITY', `Starting liquidity addition for pool ${poolAddressStr.slice(0, 8)}...`);

    const poolState = await cpAmm2.fetchPoolState(new PublicKey(poolAddress));
    const tokenAMetadata = await getTokenMetadata(poolState.tokenAMint, new PublicKey(poolAddress));
    const tokenBMetadata = await getTokenMetadata(poolState.tokenBMint, new PublicKey(poolAddress));

    logActivity('LIQUIDITY', `Pool tokens identified: ${tokenAMetadata.symbol}-${tokenBMetadata.symbol}`, {
      poolAddress: poolAddressStr,
      tokenA: tokenAMetadata.symbol,
      tokenB: tokenBMetadata.symbol,
      slippageTolerance: SLIPPAGE + '%'
    });

    const tokenAAccount = getAssociatedTokenAddressSync(poolState.tokenAMint, wallet.publicKey);
    const tokenBAccount = getAssociatedTokenAddressSync(poolState.tokenBMint, wallet.publicKey);

    const inputTokenMetadata = IS_TOKEN_A ? tokenAMetadata : tokenBMetadata;
    const inputTokenAccount = IS_TOKEN_A ? tokenAAccount : tokenBAccount;

    let INPUT_AMOUNT;
    let actualInputAmountHuman;

    // ✅ BALANCE CHECK - now inside retry wrapper
    const balance = await connection2.getTokenAccountBalance(inputTokenAccount);
    const balanceBN = new BN(balance.value.amount);
    const balanceHuman = tokenAmountToHuman(balanceBN, inputTokenMetadata.decimals);

    logActivity('LIQUIDITY', `${inputTokenMetadata.symbol} balance check`, {
      symbol: inputTokenMetadata.symbol,
      balance: balanceHuman,
      balanceRaw: balanceBN.toString()
    });

    if (balanceBN.isZero()) {
      throw new Error(`No ${inputTokenMetadata.symbol} balance available - may need time to settle`);
    }

    const percentageBN = new BN(MAX_BALANCE_PERCENTAGE);
    const hundredBN = new BN(100);
    INPUT_AMOUNT = balanceBN.mul(percentageBN).div(hundredBN);
    actualInputAmountHuman = tokenAmountToHuman(INPUT_AMOUNT, inputTokenMetadata.decimals);

    logActivity('LIQUIDITY', `Using ${MAX_BALANCE_PERCENTAGE}% of balance`, {
      percentage: MAX_BALANCE_PERCENTAGE,
      amount: actualInputAmountHuman,
      symbol: inputTokenMetadata.symbol
    });

    logActivity('LIQUIDITY', `Getting deposit quote for ${inputTokenMetadata.symbol}`, {
      inputAmount: actualInputAmountHuman,
      isTokenA: IS_TOKEN_A
    });

    // Get the initial deposit quote
    const depositQuote = await cpAmm2.getDepositQuote({
      inAmount: INPUT_AMOUNT,
      isTokenA: IS_TOKEN_A,
      minSqrtPrice: poolState.sqrtMinPrice,
      maxSqrtPrice: poolState.sqrtMaxPrice,
      sqrtPrice: poolState.sqrtPrice,
    });

    // === APPLY SLIPPAGE PROTECTION ===
    let maxAmountTokenA, maxAmountTokenB;
    let idealAmountTokenA, idealAmountTokenB;

    if (IS_TOKEN_A) {
      idealAmountTokenA = depositQuote.actualInputAmount;
      idealAmountTokenB = depositQuote.outputAmount;
      maxAmountTokenA = getMaxAmountWithSlippage(idealAmountTokenA, SLIPPAGE);
      maxAmountTokenB = getMaxAmountWithSlippage(idealAmountTokenB, SLIPPAGE);
    } else {
      idealAmountTokenA = depositQuote.outputAmount;
      idealAmountTokenB = depositQuote.actualInputAmount;
      maxAmountTokenA = getMaxAmountWithSlippage(idealAmountTokenA, SLIPPAGE);
      maxAmountTokenB = getMaxAmountWithSlippage(idealAmountTokenB, SLIPPAGE);
    }

    const tokenAAmountThreshold = getMinAmountWithSlippage(idealAmountTokenA, SLIPPAGE);
    const tokenBAmountThreshold = getMinAmountWithSlippage(idealAmountTokenB, SLIPPAGE);

    // Convert amounts to human readable for logging
    const idealAmountAHuman = tokenAmountToHuman(idealAmountTokenA, tokenAMetadata.decimals);
    const idealAmountBHuman = tokenAmountToHuman(idealAmountTokenB, tokenBMetadata.decimals);
    const maxAmountAHuman = tokenAmountToHuman(maxAmountTokenA, tokenAMetadata.decimals);
    const maxAmountBHuman = tokenAmountToHuman(maxAmountTokenB, tokenBMetadata.decimals);
    const thresholdAHuman = tokenAmountToHuman(tokenAAmountThreshold, tokenAMetadata.decimals);
    const thresholdBHuman = tokenAmountToHuman(tokenBAmountThreshold, tokenBMetadata.decimals);

    logActivity('LIQUIDITY', `Deposit quote with slippage protection calculated`, {
      liquidityDelta: depositQuote.liquidityDelta.toString(),
      slippageRate: SLIPPAGE + '%',
      idealAmounts: {
        tokenA: `${idealAmountAHuman} ${tokenAMetadata.symbol}`,
        tokenB: `${idealAmountBHuman} ${tokenBMetadata.symbol}`
      },
      maxAmounts: {
        tokenA: `${maxAmountAHuman} ${tokenAMetadata.symbol}`,
        tokenB: `${maxAmountBHuman} ${tokenBMetadata.symbol}`
      },
      thresholds: {
        tokenA: `${thresholdAHuman} ${tokenAMetadata.symbol}`,
        tokenB: `${thresholdBHuman} ${tokenBMetadata.symbol}`
      }
    });

    // ✅ CHECK FOR EXISTING POSITION USING getUserPositionByPool
    let positionNft, position, positionNftAccount;
    let isUsingExistingPosition = false;

    logActivity('LIQUIDITY', `Checking for existing user positions in pool`, {
      poolAddress: poolAddressStr,
      user: wallet.publicKey.toBase58()
    });

    try {
      const existingPositions = await cpAmm2.getUserPositionByPool(
        new PublicKey(poolAddress), 
        wallet.publicKey
      );

      if (existingPositions && existingPositions.length > 0) {
        // Use the first existing position
        const existingPosition = existingPositions[0];
        positionNft = { publicKey: existingPosition.positionNftAccount }; // Note: using positionNftAccount as the NFT
        position = existingPosition.position;
        positionNftAccount = existingPosition.positionNftAccount;
        isUsingExistingPosition = true;

        logSuccess('LIQUIDITY', `Found existing position in pool - will add liquidity to existing position`, {
          poolAddress: poolAddressStr,
          existingPositions: existingPositions.length,
          usingPosition: position.toBase58().slice(0, 8) + '...',
          positionNftAccount: positionNftAccount.toBase58().slice(0, 8) + '...',
          note: 'Reusing existing position instead of creating new one'
        });
      } else {
        logActivity('LIQUIDITY', `No existing positions found - will create new position`, {
          poolAddress: poolAddressStr,
          existingPositionsCount: 0
        });
      }
    } catch (error) {
      logWarning('LIQUIDITY', `Error checking existing positions - will create new position`, {
        error: error.message,
        poolAddress: poolAddressStr,
        fallbackAction: 'create_new_position'
      });
    }

    // ✅ CREATE NEW POSITION IF NO EXISTING POSITION FOUND
    if (!isUsingExistingPosition) {
      positionNft = Keypair.generate();
      position = PublicKey.findProgramAddressSync(
        [Buffer.from("position"), positionNft.publicKey.toBuffer()],
        DAMM_PROGRAM_ID
      )[0];
      positionNftAccount = derivePositionNftAccount(positionNft.publicKey);

      logActivity('LIQUIDITY', `Creating new position NFT: ${positionNft.publicKey.toBase58().slice(0, 8)}...`);

      const createTx = await cpAmm2.createPosition({
        owner: wallet.publicKey,
        payer: wallet.publicKey,
        pool: new PublicKey(poolAddress),
        positionNft: positionNft.publicKey,
      });

      await sendAndConfirmTransaction(connection2, createTx, [wallet, positionNft]);
      
      logSuccess('LIQUIDITY', `Position NFT created: ${positionNft.publicKey.toBase58().slice(0, 8)}...`, {
        positionNft: positionNft.publicKey.toBase58(),
        position: position.toBase58(),
        positionNftAccount: positionNftAccount.toBase58()
      });
    }

    // Execute add liquidity (same for both new and existing positions)
    logActivity('LIQUIDITY', `Executing add liquidity with slippage protection for ${inputTokenMetadata.symbol}`, {
      poolAddress: poolAddressStr,
      positionNft: positionNft.publicKey ? positionNft.publicKey.toBase58() : 'existing',
      position: position.toBase58().slice(0, 8) + '...',
      slippageProtection: SLIPPAGE + '%',
      isUsingExistingPosition: isUsingExistingPosition
    });

    const addLiquidityTx = await cpAmm2.addLiquidity({
      owner: wallet.publicKey,
      pool: new PublicKey(poolAddress),
      position,
      positionNftAccount,
      liquidityDelta: depositQuote.liquidityDelta,
      maxAmountTokenA,
      maxAmountTokenB,
      tokenAAmountThreshold,
      tokenBAmountThreshold,
      tokenAMint: poolState.tokenAMint,
      tokenBMint: poolState.tokenBMint,
      tokenAVault: poolState.tokenAVault,
      tokenBVault: poolState.tokenBVault,
      tokenAProgram: TOKEN_PROGRAM_ID,
      tokenBProgram: TOKEN_PROGRAM_ID,
      systemProgram: SystemProgram.programId,
      payer: wallet.publicKey,
    });

    const signature = await sendAndConfirmTransaction(connection2, addLiquidityTx, [wallet]);

    // Calculate price impact after transaction
    const priceImpactA = getPriceImpact(tokenAAmountThreshold, idealAmountTokenA);
    const priceImpactB = getPriceImpact(tokenBAmountThreshold, idealAmountTokenB);

    const positionIdentifier = positionNft.publicKey ? 
      positionNft.publicKey.toBase58() : 
      position.toBase58();

    logLiquidityOperation(inputTokenMetadata.symbol, poolAddressStr, 'ADD_LIQUIDITY', 'SUCCESS', {
      signature,
      positionNft: positionIdentifier.slice(0, 8) + '...',
      amount: actualInputAmountHuman,
      slippageUsed: SLIPPAGE + '%',
      priceImpact: {
        tokenA: priceImpactA.toFixed(4) + '%',
        tokenB: priceImpactB.toFixed(4) + '%'
      },
      finalAmounts: {
        tokenA: `-${thresholdAHuman} ${tokenAMetadata.symbol}`,
        tokenB: `-${thresholdBHuman} ${tokenBMetadata.symbol}`
      },
      positionType: isUsingExistingPosition ? 'EXISTING_REUSED' : 'NEW_CREATED'
    });

    const result = {
      signature,
      positionNft: positionIdentifier,
      poolAddress: poolAddressStr,
      amountAdded: actualInputAmountHuman,
      tokenSymbol: inputTokenMetadata.symbol,
      slippageUsed: SLIPPAGE,
      priceImpact: {
        tokenA: getPriceImpact(tokenAAmountThreshold, idealAmountTokenA),
        tokenB: getPriceImpact(tokenBAmountThreshold, idealAmountTokenB)
      },
      positionType: isUsingExistingPosition ? 'existing' : 'new'
    };

    logSuccess('LIQUIDITY', `Liquidity addition completed with slippage protection for ${inputTokenMetadata.symbol}`, result);
    return result;
  };

  // ✅ APPLY RETRY TO ENTIRE FUNCTION including balance check and position detection
  try {
    return await retryWithDetailedLogging(
      liquidityOperationWithRetry,        // ← ENTIRE function dalam retry
      MAX_LIQUIDITY_RETRIES,             // ← 7 retries
      RETRY_DELAY,                       // ← 3000ms delay
      `Add Liquidity Full (${poolAddressStr.slice(0, 8)})`,
      'LIQUIDITY',
      {
        poolAddress: poolAddressStr,
        slippageProtection: SLIPPAGE + '%',
        maxRetries: MAX_LIQUIDITY_RETRIES,
        note: 'Full retry including balance checks and existing position detection'
      }
    );
  } catch (error) {
    // Enhanced error logging for different failure types
    if (error.message.includes('could not find account')) {
      logError('LIQUIDITY', `Token account never became available after ${MAX_LIQUIDITY_RETRIES} attempts`, error, {
        poolAddress: poolAddressStr,
        maxRetries: MAX_LIQUIDITY_RETRIES,
        suggestion: 'Token account may need more time to settle after swap'
      });
    } else if (error.message.includes('slippage') || error.message.includes('threshold') || error.message.includes('exceeded')) {
      logError('LIQUIDITY', `Slippage protection triggered after ${MAX_LIQUIDITY_RETRIES} attempts`, error, {
        poolAddress: poolAddressStr,
        slippageUsed: SLIPPAGE + '%',
        suggestion: 'Consider increasing SLIPPAGE tolerance',
        maxRetries: MAX_LIQUIDITY_RETRIES
      });
    } else {
      logError('LIQUIDITY', `Add liquidity final failure after ${MAX_LIQUIDITY_RETRIES} attempts`, error, {
        poolAddress: poolAddressStr,
        maxRetries: MAX_LIQUIDITY_RETRIES,
        note: 'All retry attempts exhausted'
      });
    }

    throw error;
  }
}

// === POOL FUNCTIONS ===
function filterPools(pools) {
  return pools.filter(pool => {
    const meetsFeeCriteria = pool.fee > 3;
    const meetsCollectMode = pool.collectMode === 1;
    
    const endsWithBonk = typeof pool.mint === 'string' && /bonk$/.test(pool.mint);
    const endsWithBAGS = typeof pool.mint === 'string' && /BAGS$/.test(pool.mint);
    const meetsDexCriteria = !endsWithBonk && !endsWithBAGS && pool.dex !== "letsbonk.fun";

    const meetsPumpfunCriteria = (() => {
      if (pool.dex === "pumpfun-amm" && pool.mint) {
        return /pump$/.test(pool.mint);
      }
      return true;
    })();

    // Tambahan logika untuk first_pool pumpfun-amm
    const meetsFirstPoolCriteria = (() => {
      if (pool.dex === "pumpfun-amm" && pool.first_pool) {
        // Skip jika first_pool TIDAK berakhiran "pump"
        return /pump$/.test(pool.first_pool);
      }
      return true; // Untuk dex lain, lolos filter
    })();

    return meetsFeeCriteria && meetsCollectMode && meetsDexCriteria && meetsPumpfunCriteria && meetsFirstPoolCriteria;
  });
}

// === QUEUE MANAGEMENT ===
function addToQueue(pools) {
  pools.forEach(pool => {
    const poolAddress = pool.pool_address;
    const alreadyInQueue = poolQueue.some(queuedPool => queuedPool.pool_address === poolAddress);
    const poolState = poolProcessingState.get(poolAddress);

    if (poolState === "completed" || poolState === "failed") {
      console.log(`📋 Skipping ${pool.symbol} - already ${poolState}`);
      return;
    }

    if (alreadyInQueue) {
      console.log(`📋 Skipping ${pool.symbol} - already in queue`);
      return;
    }

    poolQueue.push(pool);
    poolProcessingState.set(poolAddress, "pending");
    console.log(`📥 Added to queue: ${pool.symbol} (${poolAddress.slice(0, 8)}...)`);
  });

  console.log(`📋 Queue status: ${poolQueue.length} pools waiting`);
}

// === POOL QUEUE PROCESSING ===
async function processPoolQueue() {
  if (isProcessingPool || poolQueue.length === 0) {
    return;
  }

  isProcessingPool = true;
  const currentPool = poolQueue.shift();
  const poolAddress = currentPool.pool_address;

  // ✅ Generate mint-based symbol if null
  let poolSymbol = currentPool.symbol;
  if (!poolSymbol || poolSymbol === 'null') {
    poolSymbol = `TOKEN_${currentPool.mint?.slice(0, 8)}`;
    console.log(`🔧 Generated symbol: ${poolSymbol} for mint ${currentPool.mint}`);
    
    // Update the pool object dan symbol_global
    currentPool.symbol = poolSymbol;
    if (currentPool.mint) {
      symbol_global.set(currentPool.mint, poolSymbol);
    }
  }

  logPoolProcessing('QUEUE_START', poolSymbol, poolAddress, 'PROCESSING', {
    queueRemaining: poolQueue.length,
    mint: currentPool.mint,
    dex: currentPool.dex,
    symbolGenerated: !currentPool.symbol || currentPool.symbol === 'null'
  });

  try {
    const currentState = poolProcessingState.get(poolAddress);
    const retryCount = poolRetryCount.get(poolAddress) || 0;

    logActivity('POOL', `Pool state check: ${currentPool.symbol}`, {
      state: currentState || "new",
      retryCount,
      maxRetries: MAX_POOL_RETRIES
    });

    if (currentState === "completed") {
      logPoolProcessing('SKIP', currentPool.symbol, poolAddress, 'ALREADY_COMPLETED');
      return;
    }

    if (currentState === "failed") {
      logPoolProcessing('SKIP', currentPool.symbol, poolAddress, 'PERMANENTLY_FAILED');
      return;
    }

    if (currentState === "failed_recovered") {
      logPoolProcessing('SKIP', currentPool.symbol, poolAddress, 'FAILED_BUT_RECOVERED');
      return;
    }

    if (currentState === "failed_stuck") {
      logPoolProcessing('SKIP', currentPool.symbol, poolAddress, 'FAILED_WITH_STUCK_TOKENS');
      return;
    }

    if (retryCount >= MAX_POOL_RETRIES) {
      logPoolProcessing('FAIL', currentPool.symbol, poolAddress, 'MAX_RETRIES_EXCEEDED', {
        retryCount,
        maxRetries: MAX_POOL_RETRIES
      });
      poolProcessingState.set(poolAddress, "failed");
      return;
    }

    if (currentState === "swapped") {
      logPoolProcessing('RETRY_LIQUIDITY', currentPool.symbol, poolAddress, 'LIQUIDITY_RETRY', {
        retryCount
      });

      const liquidityResult = await addSingleSidedLiquidity(poolAddress);

      poolProcessingState.set(poolAddress, "completed");
      poolRetryCount.delete(poolAddress);
      positionCount++;

      logPoolProcessing('COMPLETE', currentPool.symbol, poolAddress, 'LIQUIDITY_RETRY_SUCCESS', {
        signature: liquidityResult.signature,
        positionNft: liquidityResult.positionNft,
        totalPositions: positionCount
      });

      return;
    }

    // Step 1: AutoSwap
    const dexName = currentPool.dex || 'unknown';
    const swapConfig = getSwapAmountForDex(dexName);

    logPoolProcessing('SWAP_START', currentPool.symbol, poolAddress, 'STARTING_DEX_SPECIFIC_SWAP', {
      dex: dexName,
      swapAmount: swapConfig.amountSOL + ' SOL',
      configSource: swapConfig.source
    });

    const swapSignature = await autoSwap({
      inputMint: SOL_MINT,
      outputMint: currentPool.mint,
      poolData: currentPool, // Pass entire pool data
      signer: wallet,
    });

    poolProcessingState.set(poolAddress, "swapped");
    logPoolProcessing('SWAP_COMPLETE', currentPool.symbol, poolAddress, 'SWAP_SUCCESS', {
      signature: swapSignature
    });

    logActivity('POOL', `Waiting for swap to settle: ${currentPool.symbol}`, { settleTime: 3000 });
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Step 2: Add Liquidity
    logPoolProcessing('LIQUIDITY_START', currentPool.symbol, poolAddress, 'STARTING_LIQUIDITY');

    const liquidityResult = await addSingleSidedLiquidity(poolAddress);

    poolProcessingState.set(poolAddress, "completed");
    poolRetryCount.delete(poolAddress);
    positionCount++;

    logPoolProcessing('COMPLETE', currentPool.symbol, poolAddress, 'FULL_SUCCESS', {
      swapSignature,
      liquiditySignature: liquidityResult.signature,
      positionNft: liquidityResult.positionNft,
      totalPositions: positionCount
    });

  } catch (error) {
    const currentState = poolProcessingState.get(poolAddress);
    const currentRetryCount = poolRetryCount.get(poolAddress) || 0;
    const newRetryCount = currentRetryCount + 1;

    poolRetryCount.set(poolAddress, newRetryCount);

    logError('POOL', `Pool processing failed: ${currentPool.symbol}`, error, {
      poolAddress,
      currentState,
      newRetryCount,
      maxRetries: MAX_POOL_RETRIES
    });

    if (newRetryCount >= MAX_POOL_RETRIES) {
      if (currentState === "swapped") {
        logPoolProcessing('RECOVERY_START', currentPool.symbol, poolAddress, 'STARTING_RECOVERY', {
          reason: 'LIQUIDITY_FAILED_MAX_RETRIES'
        });

        const recoverySignature = await swapBackToSOL(currentPool.mint, currentPool.symbol);

        if (recoverySignature) {
          poolProcessingState.set(poolAddress, "failed_recovered");
          logPoolProcessing('RECOVERY_SUCCESS', currentPool.symbol, poolAddress, 'TOKENS_RECOVERED', {
            recoverySignature
          });
        } else {
          poolProcessingState.set(poolAddress, "failed_stuck");
          logPoolProcessing('RECOVERY_FAILED', currentPool.symbol, poolAddress, 'TOKENS_STUCK', {
            warning: 'Manual intervention needed'
          });
        }
      } else {
        poolProcessingState.set(poolAddress, "failed");
        logPoolProcessing('FAIL', currentPool.symbol, poolAddress, 'SWAP_FAILED_MAX_RETRIES', {
          newRetryCount,
          maxRetries: MAX_POOL_RETRIES
        });
      }
    } else {
      if (currentState === "swapped") {
        logPoolProcessing('RETRY_QUEUE', currentPool.symbol, poolAddress, 'LIQUIDITY_RETRY_QUEUED', {
          attempt: newRetryCount,
          maxRetries: MAX_POOL_RETRIES
        });
        poolQueue.unshift(currentPool);
      } else {
        poolProcessingState.set(poolAddress, "pending");
        logPoolProcessing('RETRY_QUEUE', currentPool.symbol, poolAddress, 'FULL_RETRY_QUEUED', {
          attempt: newRetryCount,
          maxRetries: MAX_POOL_RETRIES
        });
        poolQueue.unshift(currentPool);
      }
    }
  } finally {
    isProcessingPool = false;

    if (poolQueue.length > 0) {
      logActivity('POOL', `Continuing queue processing`, {
        remainingPools: poolQueue.length,
        nextPool: poolQueue[0]?.symbol,
        delay: 3000
      });

      setTimeout(() => {
        processPoolQueue();
      }, 3000);
    } else {
      logSuccess('POOL', 'Queue processing completed - no more pools to process', {
        totalProcessed: positionCount,
        completedPools: Array.from(poolProcessingState.values()).filter(state => state === "completed").length
      });
    }
  }
}

let isDisplayingPositions = false;

async function displayActivePositions() {
  // Prevent overlapping executions
  if (isDisplayingPositions) {
    console.log(`⚠️ Position monitoring already in progress, skipping this cycle...`);
    return;
  }

  isDisplayingPositions = true;
  const startTime = Date.now();

  try {
    console.log(`\n📊 === ACTIVE POSITIONS [${getCurrentTimestamp()}] ===`);

    const userPositions = await cpAmm.getPositionsByUser(wallet.publicKey);
    console.log(`👤 User has ${userPositions.length} total positions`);

    if (userPositions.length === 0) {
      console.log("ℹ️ No active positions found");
      return;
    }

    // === ENHANCED BATCH CONFIGURATION ===
    const BATCH_SIZE = 5; // Process 5 positions per batch
    const BATCH_DELAY = 3000; // 3 seconds delay between batches
    const POSITION_DELAY = 1000; // 1 second delay between positions in same batch

    // Variables for summary
    let totalFeeASum = new BN(0);
    let totalFeeBSum = new BN(0);
    const feesBySymbol = new Map(); // symbol -> amount

    // Split positions into batches
    const batches = [];
    for (let i = 0; i < userPositions.length; i += BATCH_SIZE) {
      batches.push(userPositions.slice(i, i + BATCH_SIZE));
    }

    const totalBatches = batches.length;
    const estimatedTime = (totalBatches * BATCH_DELAY) + (userPositions.length * POSITION_DELAY);

    console.log(`🔄 Processing ${userPositions.length} positions in ${totalBatches} batches (${BATCH_SIZE} positions per batch)`);
    console.log(`⏱️ Estimated completion time: ${Math.round(estimatedTime / 1000)} seconds`);
    console.log(`🚫 Position monitoring is LOCKED during this process to prevent overlap`);

    // Process each batch sequentially
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      const batchNumber = batchIndex + 1;

      console.log(`\n📦 === BATCH ${batchNumber}/${totalBatches} (${batch.length} positions) ===`);

      // Process positions in current batch with individual delays
      for (let posIndex = 0; posIndex < batch.length; posIndex++) {
        const { positionNftAccount, position, positionState } = batch[posIndex];
        const globalIndex = batchIndex * BATCH_SIZE + posIndex + 1;

        try {
          console.log(`\n[${globalIndex}/${userPositions.length}] Position: ${position.toBase58()}`);
          console.log(`    NFT Account: ${positionNftAccount.toBase58()}`);

          // Get pool state to get token info
          const poolState = await cpAmm.fetchPoolState(positionState.pool);
          const tokenAMetadata = await getTokenMetadata(poolState.tokenAMint, positionState.pool);
          const tokenBMetadata = await getTokenMetadata(poolState.tokenBMint, positionState.pool);

          console.log(`    Pool: ${tokenAMetadata.symbol}-${tokenBMetadata.symbol}`);
          console.log(`    Pool Address: ${positionState.pool.toBase58()}`);

          // Calculate unclaimed fees using getUnClaimReward function
          try {
            // First we need to fetch the position state properly
            const [positionPDA] = PublicKey.findProgramAddressSync(
              [Buffer.from("position"), positionState.nftMint.toBuffer()],
              DAMM_PROGRAM_ID
            );

            // Fetch position state using the SDK
            const fullPositionState = await cpAmm.fetchPositionState(positionPDA);

            // Now calculate unclaimed rewards
            const unclaimedRewards = getUnClaimReward(poolState, fullPositionState);

            // Convert to human readable
            let feeAHuman = "0";
            let feeBHuman = "0";

            if (unclaimedRewards.feeTokenA && !unclaimedRewards.feeTokenA.isZero()) {
              feeAHuman = tokenAmountToHuman(unclaimedRewards.feeTokenA, tokenAMetadata.decimals);

              // Add to summary totals
              totalFeeASum = totalFeeASum.add(unclaimedRewards.feeTokenA);

              // Add to symbol-based summary
              const currentAmount = feesBySymbol.get(tokenAMetadata.symbol) || new BN(0);
              feesBySymbol.set(tokenAMetadata.symbol, currentAmount.add(unclaimedRewards.feeTokenA));
            }

            if (unclaimedRewards.feeTokenB && !unclaimedRewards.feeTokenB.isZero()) {
              feeBHuman = tokenAmountToHuman(unclaimedRewards.feeTokenB, tokenBMetadata.decimals);

              // Add to summary totals
              totalFeeBSum = totalFeeBSum.add(unclaimedRewards.feeTokenB);

              // Add to symbol-based summary
              const currentAmount = feesBySymbol.get(tokenBMetadata.symbol) || new BN(0);
              feesBySymbol.set(tokenBMetadata.symbol, currentAmount.add(unclaimedRewards.feeTokenB));
            }

            console.log(`    💰 Unclaimed Fees: ${feeAHuman} ${tokenAMetadata.symbol}, ${feeBHuman} ${tokenBMetadata.symbol}`);

          } catch (rewardError) {
            console.log(`    ⚠️ Error calculating unclaimed rewards: ${rewardError.message}`);
            console.log(`    💰 Fees: Unable to calculate`);
          }

        } catch (error) {
          console.log(`\n[${globalIndex}/${userPositions.length}] Position: ${position.toBase58()}`);
          console.log(`    NFT Account: ${positionNftAccount.toBase58()}`);
          console.log(`    ⚠️ Error fetching details: ${error.message}`);
        }

        // Add delay between positions in same batch (except for last position in batch)
        if (posIndex < batch.length - 1) {
          console.log(`    ⏳ Position delay: ${POSITION_DELAY}ms...`);
          await new Promise(resolve => setTimeout(resolve, POSITION_DELAY));
        }
      }

      // Add delay between batches (except for last batch)
      if (batchIndex < batches.length - 1) {
        const remainingBatches = totalBatches - batchNumber;
        const remainingPositions = userPositions.length - (batchNumber * BATCH_SIZE);

        console.log(`\n⏳ Batch ${batchNumber}/${totalBatches} completed. Waiting ${BATCH_DELAY}ms before next batch...`);

        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
      }
    }

    // === FINAL SUMMARY SECTION ===
    const endTime = Date.now();
    const actualDuration = Math.round((endTime - startTime) / 1000);

    console.log(`\n💰 === TOTAL UNCLAIMED FEES SUMMARY ===`);
    console.log(`📈 Total Positions: ${userPositions.length}`);
    console.log(`⏱️ Processing Time: ${actualDuration} seconds`);

    if (feesBySymbol.size > 0) {
      console.log(`💎 Fees by Token:`);
      for (const [symbol, totalAmount] of feesBySymbol.entries()) {
        // Get token metadata for decimals
        const tokenMetadata = Array.from(tokenMetadataCache.values())
          .find(meta => meta.symbol === symbol);

        if (tokenMetadata) {
          const humanAmount = tokenAmountToHuman(totalAmount, tokenMetadata.decimals);
          console.log(`  ${symbol}: ${humanAmount}`);
        } else {
          console.log(`  ${symbol}: ${totalAmount.toString()} (raw)`);
        }
      }

      // Special handling for SOL (most common)
      const solAmount = feesBySymbol.get('SOL');
      if (solAmount) {
        const solHuman = tokenAmountToHuman(solAmount, 9);
        console.log(`🔥 Total SOL Fees: ${solHuman} SOL`);
      }

    } else {
      console.log(`ℹ️ No unclaimed fees found across all positions`);
    }

    // === STATUS SUMMARY ===
    console.log(`\n📋 === DAEMON STATUS ===`);
    console.log(`🔄 Queue Status: ${poolQueue.length} pools waiting`);
    console.log(`✅ Completed Pools: ${Array.from(poolProcessingState.values()).filter(state => state === "completed").length}`);
    console.log(`🔄 Swapped Pools: ${Array.from(poolProcessingState.values()).filter(state => state === "swapped").length}`);
    console.log(`❌ Failed Pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed").length}`);
    console.log(`🔄 Recovered Pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed_recovered").length}`);
    console.log(`⚠️ Stuck Pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed_stuck").length}`);
    console.log(`📈 Active Positions: ${userPositions.length}`);

    console.log(`\n✅ === POSITION MONITORING COMPLETED ===`);
    console.log(`🔓 Position monitoring is now UNLOCKED for next cycle`);

  } catch (error) {
    console.error("❌ Error displaying positions:", error.message);
    console.log(`🔓 Position monitoring UNLOCKED due to error`);
  } finally {
    // Always unlock the flag, even if there was an error
    isDisplayingPositions = false;
  }
}

// === FILE MONITORING ===
function hasFileChanged() {
  try {
    if (!fs.existsSync(SENT_POOLS_FILE)) {
      return false;
    }

    const stats = fs.statSync(SENT_POOLS_FILE);
    const currentModified = stats.mtime.getTime();

    if (currentModified !== lastFileModified) {
      lastFileModified = currentModified;
      return true;
    }

    return false;
  } catch (error) {
    console.error("❌ Error checking file:", error.message);
    return false;
  }
}

async function checkAndProcessPools() {
  try {
    if (!hasFileChanged()) {
      return;
    }

    if (!snapshotCreated) {
      console.log(`⏳ Snapshot not ready yet, skipping pool check...`);
      return;
    }

    console.log(`\n🔄 === POOL CHECK [${getCurrentTimestamp()}] ===`);
    console.log("📄 File updated, checking for new pools...");

    poolsData = JSON.parse(fs.readFileSync(SENT_POOLS_FILE, 'utf8'));
    updateSymbolGlobal();
    const eligiblePools = filterPools(poolsData);

    const trulyNewPools = eligiblePools.filter(pool => {
      const isNotInSnapshot = !initialPoolSnapshot.has(pool.pool_address);
      const poolState = poolProcessingState.get(pool.pool_address);
      const isNotProcessed = !poolState || poolState === "pending";
      const isNotInQueue = !poolQueue.some(queuedPool => queuedPool.pool_address === pool.pool_address);

      return isNotInSnapshot && isNotProcessed && isNotInQueue;
    });

    console.log(`📋 Total pools in file: ${poolsData.length}`);
    console.log(`🎯 Eligible pools: ${eligiblePools.length}`);
    console.log(`📸 Initial snapshot: ${initialPoolSnapshot.size} pools`);
    console.log(`🆕 Truly new pools: ${trulyNewPools.length}`);
    console.log(`📋 Queue: ${poolQueue.length} waiting`);
    console.log(`✅ Completed pools: ${Array.from(poolProcessingState.values()).filter(state => state === "completed").length}`);
    console.log(`🔄 Swapped pools: ${Array.from(poolProcessingState.values()).filter(state => state === "swapped").length}`);
    console.log(`❌ Failed pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed").length}`);
    console.log(`🔄 Recovered pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed_recovered").length}`);
    console.log(`⚠️ Stuck pools: ${Array.from(poolProcessingState.values()).filter(state => state === "failed_stuck").length}`);

    if (trulyNewPools.length === 0) {
      console.log("ℹ️ No truly new pools to add to queue (skipping initial snapshot)");
      return;
    }

    console.log(`🆕 New pools detected:`);
    trulyNewPools.forEach((pool, i) => {
      console.log(`  ${i + 1}. ${pool.symbol} (${pool.pool_address.slice(0, 8)}...) - ${pool.dex}`);
    });

    addToQueue(trulyNewPools);

    if (!isProcessingPool && poolQueue.length > 0) {
      console.log(`🚀 Starting queue processing...`);
      processPoolQueue();
    }

  } catch (error) {
    console.error("❌ Error checking pools:", error.message);
  }
}

// === FIXED startDaemon() FUNCTION ===
async function startDaemon() {
  startLogMonitoring();

  logActivity('SYSTEM', 'Pool Orchestrator Daemon Starting', {
    version: 'Enhanced with DEX-Specific Amounts + Comprehensive Logging',
    poolCheckInterval: POOL_CHECK_INTERVAL,
    positionDisplayInterval: POSITION_DISPLAY_INTERVAL,
    dexConfigCount: Object.keys(DEX_SWAP_AMOUNTS).length - 1,
    monitoringFile: SENT_POOLS_FILE
  });

  console.log("🚀 Pool Orchestrator Daemon Started (Enhanced with DEX-Specific Amounts)");
  console.log("=".repeat(70));
  
  // Display DEX configuration
  displayDexConfiguration();
  
  console.log("=".repeat(70));
  console.log(`⏰ Pool check interval: ${POOL_CHECK_INTERVAL / 1000}s`);
  console.log(`📊 Position display interval: ${POSITION_DISPLAY_INTERVAL / 1000}s`);
  console.log(`📁 Monitoring file: ${SENT_POOLS_FILE}`);
  console.log(`📋 Logging directory: ./logs/`);
  console.log(`🔄 Processing mode: Enhanced Sequential Queue with DEX-Specific Amounts + Comprehensive Logging`);
  console.log("=".repeat(70));

  displayLogStats();

  if (!PRIVATE_KEY_BS58) {
    const error = new Error("PRIVATE_KEY_BS58 tidak boleh kosong!");
    logError('SYSTEM', 'Missing private key configuration', error);
    throw error;
  }

  const solBalance = await connection.getBalance(wallet.publicKey);
  logActivity('SYSTEM', 'Initial SOL balance check', {
    balance: solBalance / 1e9,
    balanceRaw: solBalance,
    wallet: wallet.publicKey.toBase58()
  });

  if (solBalance < 0.01 * 1e9) {
    const error = new Error("Insufficient SOL balance. Need at least 0.01 SOL");
    logError('SYSTEM', 'Insufficient SOL balance', error, {
      currentBalance: solBalance / 1e9,
      requiredBalance: 0.01
    });
    throw error;
  }

  logActivity('SYSTEM', 'Testing RPC connections');
  try {
    const slot1 = await connection.getSlot();
    const slot2 = await connection2.getSlot();

    logSuccess('SYSTEM', 'RPC connections successful', {
      mainRPCSlot: slot1,
      swapRPCSlot: slot2,
      slotDifference: Math.abs(slot1 - slot2)
    });

    if (Math.abs(slot1 - slot2) > 10) {
      logWarning('SYSTEM', 'RPC slots significantly different', {
        mainRPCSlot: slot1,
        swapRPCSlot: slot2,
        difference: Math.abs(slot1 - slot2)
      });
    }
  } catch (error) {
    logError('SYSTEM', 'RPC connection test failed', error);
    throw new Error("Failed to connect to one or both RPC endpoints");
  }

  if (fs.existsSync(SENT_POOLS_FILE)) {
    const stats = fs.statSync(SENT_POOLS_FILE);
    lastFileModified = stats.mtime.getTime();
    poolsData = JSON.parse(fs.readFileSync(SENT_POOLS_FILE, 'utf8'));
    updateSymbolGlobal();

    logActivity('SYSTEM', 'File initialized and symbol mapping updated', {
      fileModified: new Date(lastFileModified).toLocaleString(),
      totalPools: poolsData.length,
      symbolMappings: symbol_global.size
    });

    const eligiblePools = filterPools(poolsData);
    eligiblePools.forEach(pool => {
      initialPoolSnapshot.add(pool.pool_address);
      poolProcessingState.set(pool.pool_address, "completed");
    });

    snapshotCreated = true;

    logActivity('SYSTEM', 'Initial snapshot created', {
      totalPoolsInFile: poolsData.length,
      eligiblePools: eligiblePools.length,
      snapshotSize: initialPoolSnapshot.size,
      strategy: 'Only new pools will be processed'
    });
  }

  await displayActivePositions();

  logSuccess('SYSTEM', 'Daemon ready - waiting for new pools', {
    monitoringFile: SENT_POOLS_FILE,
    initialSnapshot: initialPoolSnapshot.size,
    dexConfiguredCount: Object.keys(DEX_SWAP_AMOUNTS).length - 1
  });

  const poolCheckInterval = setInterval(checkAndProcessPools, POOL_CHECK_INTERVAL);
  const positionDisplayInterval = setInterval(displayActivePositions, POSITION_DISPLAY_INTERVAL);

  logActivity('SYSTEM', 'Daemon intervals started', {
    poolCheckInterval: POOL_CHECK_INTERVAL,
    positionDisplayInterval: POSITION_DISPLAY_INTERVAL,
    dexSwapAmounts: DEX_SWAP_AMOUNTS
  });

  process.on('SIGINT', () => {
    logActivity('SYSTEM', 'Shutdown signal received');
    console.log('\n🛑 Shutting down daemon...');

    clearInterval(poolCheckInterval);
    clearInterval(positionDisplayInterval);

    const finalStats = {
      initialSnapshot: initialPoolSnapshot.size,
      completed: Array.from(poolProcessingState.values()).filter(state => state === "completed").length,
      swapped: Array.from(poolProcessingState.values()).filter(state => state === "swapped").length,
      failed: Array.from(poolProcessingState.values()).filter(state => state === "failed").length,
      recovered: Array.from(poolProcessingState.values()).filter(state => state === "failed_recovered").length,
      stuck: Array.from(poolProcessingState.values()).filter(state => state === "failed_stuck").length,
      queueRemaining: poolQueue.length,
      totalPositions: positionCount,
      dexConfigCount: Object.keys(DEX_SWAP_AMOUNTS).length - 1
    };

    logActivity('SYSTEM', 'Final daemon statistics', finalStats);
    displayLogStats();

    console.log('\n📊 === FINAL STATISTICS ===');
    Object.entries(finalStats).forEach(([key, value]) => {
      console.log(`${key}: ${value}`);
    });

    logSuccess('SYSTEM', 'Daemon stopped gracefully');
    process.exit(0);
  });

  process.stdin.resume();
}

// === START DAEMON ===
startDaemon().catch(error => {
  logError('SYSTEM', 'Daemon failed to start', error, {
    timestamp: getDetailedTimestamp(),
    nodeVersion: process.version,
    platform: process.platform
  });
  console.error("💥 Daemon failed to start:", error.message);
  process.exit(1);
});
