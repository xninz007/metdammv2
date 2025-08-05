import fs from 'fs';
import {
  Connection,
  PublicKey,
  Keypair,
  sendAndConfirmTransaction,
  SystemProgram,
  VersionedTransaction,
} from "@solana/web3.js";
import {
  TOKEN_PROGRAM_ID,
  ASSOCIATED_TOKEN_PROGRAM_ID,
  getAssociatedTokenAddressSync,
  getMint,
} from "@solana/spl-token";
import { CpAmm, getUnClaimReward } from "@meteora-ag/cp-amm-sdk";
import { BN } from "bn.js";
import bs58 from "bs58";
import axios from "axios";

// === KONFIGURASI ===
const RPC_URL = "";
const PRIVATE_KEY_BS58 = ""; // 🛑 Ganti dengan private key dari Phantom

// === KONFIGURASI CLOSE POSITIONS ===
const SLIPPAGE_TOLERANCE = 1; // 1% slippage tolerance
const BATCH_SIZE = 3; // Process 3 positions per batch
const DELAY_BETWEEN_BATCHES = 2000; // 2 seconds delay between batches
const DELAY_BETWEEN_TRANSACTIONS = 1000; // 1 second delay between transactions
const MIN_SOL_THRESHOLD = 0; // Minimum SOL value to consider for closing (in SOL)
const MIN_TOKEN_THRESHOLD = 0; // Minimum token value to consider for closing

// === KONFIGURASI AUTOSWAP ===
const AUTO_SWAP_TO_SOL = true; // Enable auto swap non-SOL tokens to SOL
const SOL_MINT = "So11111111111111111111111111111111111111112";
const MIN_SWAP_AMOUNT = 0.001; // Minimum token amount to swap (in token units)
const SWAP_SLIPPAGE = 1; // 1% slippage for swaps
const MAX_SWAP_RETRIES = 3; // Maximum retry attempts for failed swaps
const DELAY_AFTER_CLOSE = 3000; // 3 seconds delay after closing before swapping

// === INISIALISASI ===
const connection = new Connection(RPC_URL, "confirmed");
const cpAmm = new CpAmm(connection);
let wallet;
const DAMM_PROGRAM_ID = new PublicKey("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG");

// === UTILITY FUNCTIONS ===
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function tokenAmountToHuman(tokenAmount, decimals) {
  return parseFloat(tokenAmount.toString()) / Math.pow(10, decimals);
}

function derivePositionNftAccount(positionNftMint) {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("position_nft_account"), positionNftMint.toBuffer()],
    DAMM_PROGRAM_ID
  )[0];
}

// === AUTOSWAP FUNCTION WITH RETRY ===
async function autoSwap({
  inputMint,
  outputMint,
  amountInLamports,
  signer,
  maxRetries = MAX_SWAP_RETRIES
}) {
  if (!signer || !signer.publicKey) {
    throw new Error("Parameter 'signer' (Keypair) harus disediakan");
  }

  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Convert amount to proper format for logging
      const tokenAmount = parseFloat(amountInLamports) / 1e9;
      console.log(`    🔄 Swapping ${tokenAmount.toFixed(6)} tokens to SOL... (Attempt ${attempt}/${maxRetries})`);
      
      // Step 1: GET /order
      const taker = signer.publicKey.toBase58();
      const orderUrl = `https://ultra-api.jup.ag/order?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amountInLamports}&taker=${taker}&swapMode=ExactIn`;
      
      console.log(`    📡 Getting order... (Attempt ${attempt})`);
      const { data: order } = await axios.get(orderUrl);

      if (!order?.transaction || !order?.requestId) {
        throw new Error("Order response tidak valid");
      }

      console.log(`    📝 Order received, signing transaction... (Attempt ${attempt})`);
      // Step 2: Sign the transaction
      const txBuffer = Buffer.from(order.transaction, "base64");
      const tx = VersionedTransaction.deserialize(txBuffer);
      tx.sign([signer]);

      console.log(`    🚀 Executing swap... (Attempt ${attempt})`);
      // Step 3: POST /execute
      const { data: result } = await axios.post("https://ultra-api.jup.ag/execute", {
        signedTransaction: Buffer.from(tx.serialize()).toString("base64"),
        requestId: order.requestId,
      });

      if (result.status !== "Success") {
        throw new Error(`Swap gagal: ${JSON.stringify(result)}`);
      }

      console.log(`    ✅ Swap success on attempt ${attempt}: ${result.signature.slice(0, 8)}...`);
      return result.signature;

    } catch (err) {
      lastError = err;
      const errorMessage = err?.response?.data?.error || err?.response?.data || err?.message || err.toString();
      
      console.error(`    ❌ Swap attempt ${attempt}/${maxRetries} failed: ${errorMessage}`);
      
      // Check if this is a retryable error
      const isRetryableError = 
        errorMessage.includes("Order not found") ||
        errorMessage.includes("it might have expired") ||
        errorMessage.includes("Request failed") ||
        errorMessage.includes("timeout") ||
        errorMessage.includes("network") ||
        errorMessage.includes("ETIMEDOUT") ||
        errorMessage.includes("ECONNRESET") ||
        (err?.response?.status >= 500 && err?.response?.status < 600);
      
      if (attempt < maxRetries && isRetryableError) {
        const waitTime = Math.min(1000 * Math.pow(2, attempt - 1), 10000); // Exponential backoff
        console.log(`    ⏳ Retrying in ${waitTime / 1000}s... (${maxRetries - attempt} attempts left)`);
        await sleep(waitTime);
      } else if (attempt < maxRetries) {
        console.log(`    ⚠️ Non-retryable error detected, stopping retries early`);
        break;
      }
    }
  }
  
  // All retries failed
  const finalError = lastError?.response?.data?.error || lastError?.response?.data || lastError?.message || lastError;
  throw new Error(`Swap failed after ${maxRetries} attempts: ${finalError}`);
}

// === GET TOKEN BALANCE ===
async function getTokenBalance(mintAddress, walletPublicKey) {
  try {
    const tokenAccount = getAssociatedTokenAddressSync(mintAddress, walletPublicKey);
    const balance = await connection.getTokenAccountBalance(tokenAccount);
    return {
      balance: new BN(balance.value.amount),
      decimals: balance.value.decimals,
      uiAmount: balance.value.uiAmount || 0
    };
  } catch (error) {
    // Token account doesn't exist or other error
    return {
      balance: new BN(0),
      decimals: 9,
      uiAmount: 0
    };
  }
}

// === SWAP TOKENS TO SOL ===
async function swapTokensToSol(tokenMint, tokenSymbol, tokenDecimals, minSwapAmount = MIN_SWAP_AMOUNT) {
  try {
    // Skip if it's already SOL
    if (tokenMint.toBase58() === SOL_MINT) {
      console.log(`    ⏭️ Skipping swap: ${tokenSymbol} is already SOL`);
      return { success: true, skipped: true };
    }
    
    // Get current token balance
    const tokenBalance = await getTokenBalance(tokenMint, wallet.publicKey);
    
    if (tokenBalance.uiAmount === 0 || tokenBalance.uiAmount < minSwapAmount) {
      console.log(`    ⏭️ Skipping swap: ${tokenSymbol} balance ${tokenBalance.uiAmount} below minimum ${minSwapAmount}`);
      return { success: true, skipped: true };
    }
    
    console.log(`    💰 ${tokenSymbol} balance: ${tokenBalance.uiAmount}`);
    console.log(`    🎯 Swapping 100% (${tokenBalance.uiAmount}) ${tokenSymbol} to SOL...`);
    
    // Use 100% of balance - convert entire balance to lamports (smallest unit)
    const amountInLamports = tokenBalance.balance.toString();
    
    // Perform swap with 100% of balance
    const swapSignature = await autoSwap({
      inputMint: tokenMint.toBase58(),
      outputMint: SOL_MINT,
      amountInLamports: amountInLamports,
      signer: wallet,
      maxRetries: MAX_SWAP_RETRIES
    });
    
    return { 
      success: true, 
      signature: swapSignature,
      amountSwapped: tokenBalance.uiAmount,
      tokenSymbol: tokenSymbol,
      percentageSwapped: 100
    };
    
  } catch (error) {
    console.error(`    ❌ Failed to swap ${tokenSymbol} to SOL: ${error.message}`);
    return { 
      success: false, 
      error: error.message,
      tokenSymbol: tokenSymbol
    };
  }
}

// === TOKEN METADATA CACHE ===
const tokenMetadataCache = new Map();

async function getTokenMetadata(mintAddress) {
  const mintKey = mintAddress.toBase58();
  
  if (tokenMetadataCache.has(mintKey)) {
    return tokenMetadataCache.get(mintKey);
  }

  try {
    const mintInfo = await getMint(connection, mintAddress);
    
    let symbol = "UNKNOWN";
    let name = "Unknown Token";
    
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
    return metadata;
    
  } catch (error) {
    const fallbackMetadata = {
      mint: mintAddress,
      decimals: 9,
      symbol: "UNKNOWN",
      name: "Unknown Token",
      supply: new BN(0)
    };
    
    tokenMetadataCache.set(mintKey, fallbackMetadata);
    return fallbackMetadata;
  }
}

// === FIX POOL PUBKEY ISSUE ===
async function analyzePosition(userPosition) {
  const { positionNftAccount, position, positionState } = userPosition;
  
  try {
    console.log(`🔍 Analyzing position: ${position.toBase58()}`);
    
    // Validate input
    if (!positionState || !positionState.pool) {
      console.log(`    ❌ Invalid position state - missing pool reference`);
      return null;
    }
    
    if (!positionState.nftMint) {
      console.log(`    ❌ Invalid position state - missing NFT mint`);
      return null;
    }
    
    // Get pool state with validation
    let poolState;
    try {
      poolState = await cpAmm.fetchPoolState(positionState.pool);
      
      // PERBAIKAN: Set pubkey manually jika tidak ada
      if (!poolState.pubkey) {
        poolState.pubkey = positionState.pool;
        console.log(`    🔧 Fixed missing pool pubkey: ${poolState.pubkey.toBase58()}`);
      }
      
      // Validate other required fields
      const requiredFields = ['tokenAMint', 'tokenBMint', 'tokenAVault', 'tokenBVault'];
      const missingFields = requiredFields.filter(field => !poolState[field]);
      
      if (missingFields.length > 0) {
        console.log(`    ❌ Pool state missing required fields: ${missingFields.join(', ')}`);
        console.log(`    🔍 Available pool state fields:`, Object.keys(poolState));
        return null;
      }
      
      console.log(`    ✅ Pool state validated successfully`);
      console.log(`    🏦 Pool: ${poolState.pubkey.toBase58()}`);
      console.log(`    🪙 Token A Mint: ${poolState.tokenAMint.toBase58()}`);
      console.log(`    🪙 Token B Mint: ${poolState.tokenBMint.toBase58()}`);
      console.log(`    🏦 Token A Vault: ${poolState.tokenAVault.toBase58()}`);
      console.log(`    🏦 Token B Vault: ${poolState.tokenBVault.toBase58()}`);
      
    } catch (error) {
      console.log(`    ❌ Failed to fetch pool state: ${error.message}`);
      return null;
    }
    
    // Get token metadata with fallbacks
    let tokenAMetadata, tokenBMetadata;
    
    try {
      tokenAMetadata = await getTokenMetadata(poolState.tokenAMint);
    } catch (error) {
      console.log(`    ⚠️ Using fallback for token A metadata: ${error.message}`);
      tokenAMetadata = {
        mint: poolState.tokenAMint,
        symbol: 'UNKNOWN',
        decimals: 9,
        name: 'Unknown Token'
      };
    }
    
    try {
      tokenBMetadata = await getTokenMetadata(poolState.tokenBMint);
    } catch (error) {
      console.log(`    ⚠️ Using fallback for token B metadata: ${error.message}`);
      tokenBMetadata = {
        mint: poolState.tokenBMint,
        symbol: 'UNKNOWN',
        decimals: 9,
        name: 'Unknown Token'
      };
    }
    
    console.log(`    📊 Pool: ${tokenAMetadata.symbol}-${tokenBMetadata.symbol}`);
    
    // Calculate unclaimed rewards with error handling
    let unclaimedRewards = { feeTokenA: new BN(0), feeTokenB: new BN(0) };
    let feeAHuman = 0;
    let feeBHuman = 0;
    
    try {
      const [positionPDA] = PublicKey.findProgramAddressSync(
        [Buffer.from("position"), positionState.nftMint.toBuffer()],
        DAMM_PROGRAM_ID
      );
      
      let fullPositionState;
      try {
        fullPositionState = await cpAmm.fetchPositionState(positionPDA);
      } catch (fetchError) {
        console.log(`    ⚠️ Using fallback position state: ${fetchError.message}`);
        fullPositionState = positionState;
      }
      
      unclaimedRewards = getUnClaimReward(poolState, fullPositionState);
      
      feeAHuman = unclaimedRewards.feeTokenA ? 
        tokenAmountToHuman(unclaimedRewards.feeTokenA, tokenAMetadata.decimals) : 0;
      feeBHuman = unclaimedRewards.feeTokenB ? 
        tokenAmountToHuman(unclaimedRewards.feeTokenB, tokenBMetadata.decimals) : 0;
        
      console.log(`    💰 Unclaimed fees: ${feeAHuman} ${tokenAMetadata.symbol}, ${feeBHuman} ${tokenBMetadata.symbol}`);
      
    } catch (rewardError) {
      console.log(`    ⚠️ Could not calculate rewards: ${rewardError.message}`);
      // Continue with zero fees
    }
    
    // Check if position is locked
    let isLocked = false;
    try {
      if (cpAmm.isLockedPosition) {
        isLocked = cpAmm.isLockedPosition(positionState);
      }
    } catch (lockError) {
      console.log(`    ⚠️ Could not check lock status: ${lockError.message}`);
    }
    
    const positionValue = {
      tokenA: feeAHuman,
      tokenB: feeBHuman,
      tokenASymbol: tokenAMetadata.symbol,
      tokenBSymbol: tokenBMetadata.symbol
    };
    
    // Consider position worth closing if it has ANY fees
    const isWorthClosing = !isLocked;
    
    console.log(`    🎯 Worth closing: ${isWorthClosing ? 'YES' : 'NO'} (locked: ${isLocked})`);
    
    return {
      position,
      positionNftAccount,
      positionState,
      poolState,
      tokenAMetadata,
      tokenBMetadata,
      unclaimedRewards,
      positionValue,
      isLocked,
      isWorthClosing
    };
    
  } catch (error) {
    console.error(`❌ Error analyzing position ${position.toBase58()}: ${error.message}`);
    return null;
  }
}

// === FIX BERDASARKAN DOKUMENTASI RESMI METEORA ===
// === FIX BERDASARKAN DOKUMENTASI RESMI METEORA ===
async function closeSinglePosition(positionAnalysis, index, total) {
  const { 
    position, 
    positionNftAccount, 
    positionState, 
    poolState, 
    tokenAMetadata, 
    tokenBMetadata,
    positionValue,
    isLocked
  } = positionAnalysis;

  console.log(`\n[${index + 1}/${total}] 🎯 Closing Position: ${position.toBase58()}`);
  console.log(`    Pool: ${tokenAMetadata.symbol}-${tokenBMetadata.symbol}`);
  console.log(`    NFT Account: ${positionNftAccount.toBase58()}`);
  console.log(`    Fees (est.): ${positionValue.tokenA} ${positionValue.tokenASymbol}, ${positionValue.tokenB} ${positionValue.tokenBSymbol}`);

  if (isLocked) {
    console.log(`    ⚠️ Position is locked, skipping...`);
    return { success: false, reason: 'locked' };
  }

  try {
    if (!poolState.pubkey) {
      poolState.pubkey = positionState.pool;
      console.log(`    🔧 Fixed pool pubkey: ${poolState.pubkey.toBase58()}`);
    }

    const currentSlot = await connection.getSlot();
    const params = {
      owner: wallet.publicKey,
      position,
      positionNftAccount,
      positionState,
      poolState,
      tokenAAmountThreshold: new BN(0),
      tokenBAmountThreshold: new BN(0),
      currentPoint: new BN(currentSlot),
      vestings: []
    };

    try {
      const tx = await cpAmm.removeAllLiquidityAndClosePosition(params);
      const signature = await sendAndConfirmTransaction(connection, tx, [wallet], {
        commitment: 'confirmed',
        preflightCommitment: 'confirmed',
        maxRetries: 3,
        skipPreflight: false
      });

      console.log(`    ✅ Position closed: ${signature.slice(0, 8)}...`);
      console.log(`    📝 TX: https://solscan.io/tx/${signature}`);

      const result = {
        success: true,
        signature,
        method: 'removeAllLiquidityAndClosePosition',
        transactions: [
          { type: 'removeAllLiquidityAndClosePosition', signature }
        ],
        claimedTokenA: positionValue.tokenA,
        claimedTokenB: positionValue.tokenB,
        tokenASymbol: positionValue.tokenASymbol,
        tokenBSymbol: positionValue.tokenBSymbol,
        swapResults: []
      };

      // === AUTO SWAP TO SOL ===
      if (AUTO_SWAP_TO_SOL) {
        console.log(`    ⏳ Waiting ${DELAY_AFTER_CLOSE / 1000}s for liquidity tokens to settle...`);
        await sleep(DELAY_AFTER_CLOSE);

        console.log(`    📍 Starting auto-swap for received tokens...`);

        const tokensToSwapArray = [];

        if (tokenAMetadata.mint.toBase58() !== SOL_MINT) {
          tokensToSwapArray.push({
            mint: tokenAMetadata.mint,
            symbol: tokenAMetadata.symbol,
            decimals: tokenAMetadata.decimals
          });
        }

        if (
          tokenBMetadata.mint.toBase58() !== SOL_MINT &&
          tokenBMetadata.mint.toBase58() !== tokenAMetadata.mint.toBase58()
        ) {
          tokensToSwapArray.push({
            mint: tokenBMetadata.mint,
            symbol: tokenBMetadata.symbol,
            decimals: tokenBMetadata.decimals
          });
        }

        if (tokensToSwapArray.length === 0) {
          console.log(`    ℹ️ No non-SOL tokens to swap`);
        } else {
          console.log(`    🎯 Found ${tokensToSwapArray.length} token(s) to swap to SOL`);
          for (let i = 0; i < tokensToSwapArray.length; i++) {
            const token = tokensToSwapArray[i];
            console.log(`    💱 Processing swap ${i + 1}/${tokensToSwapArray.length}: ${token.symbol} → SOL`);

            try {
              const swapResult = await swapTokensToSol(
                token.mint,
                token.symbol,
                token.decimals,
                MIN_SWAP_AMOUNT
              );

              result.swapResults.push(swapResult);

              if (swapResult.success && !swapResult.skipped) {
                console.log(`    ✅ Swapped ${swapResult.amountSwapped} ${swapResult.tokenSymbol} to SOL`);
                console.log(`    📝 Swap TX: ${swapResult.signature.slice(0, 8)}...`);
              } else if (swapResult.skipped) {
                console.log(`    ⏭️ Swap skipped: ${swapResult.tokenSymbol}`);
              } else {
                console.log(`    ❌ Swap failed: ${swapResult.tokenSymbol} - ${swapResult.error}`);
              }
            } catch (swapError) {
              console.log(`    ❌ Swap error for ${token.symbol}: ${swapError.message}`);
              result.swapResults.push({
                success: false,
                error: swapError.message,
                tokenSymbol: token.symbol
              });
            }

            if (i < tokensToSwapArray.length - 1) {
              await sleep(1000);
            }
          }

          console.log(`    🎉 Auto-swap completed!`);
        }
      }

      return result;

    } catch (txError) {
      const isInsufficientLiquidity =
        txError?.logs?.some(log => log.includes("Error Code: InsufficientLiquidity") || log.includes("Error Number: 6023")) ||
        txError?.message?.includes("6023");

      if (isInsufficientLiquidity) {
        console.log(`    ⚠️ Insufficient liquidity to remove, attempting direct close...`);

        try {
          const [positionPDA] = PublicKey.findProgramAddressSync(
            [Buffer.from("position"), positionState.nftMint.toBuffer()],
            DAMM_PROGRAM_ID
          );
          const updatedPositionState = await cpAmm.fetchPositionState(positionPDA);

          const closeTx = await cpAmm.closePosition({
            owner: wallet.publicKey,
            pool: poolState.pubkey,
            position: position,
            positionNftMint: updatedPositionState.nftMint,
            positionNftAccount: positionNftAccount
          });

          const closeSignature = await sendAndConfirmTransaction(connection, closeTx, [wallet], {
            commitment: 'confirmed',
            preflightCommitment: 'confirmed',
            maxRetries: 3,
            skipPreflight: false
          });

          console.log(`    ✅ Empty position closed: ${closeSignature.slice(0, 8)}...`);

          return {
            success: true,
            signature: closeSignature,
            method: 'direct_close_no_liquidity',
            transactions: [
              { type: 'closePosition', signature: closeSignature }
            ],
            claimedTokenA: 0,
            claimedTokenB: 0,
            tokenASymbol: positionValue.tokenASymbol,
            tokenBSymbol: positionValue.tokenBSymbol,
            swapResults: []
          };

        } catch (closeError) {
          console.log(`    ⚠️ Could not close empty position: ${closeError.message}`);
          return {
            success: false,
            reason: 'Could not close empty position',
            fullError: closeError,
            positionAddress: position.toBase58(),
            method: 'fees_claimed_empty_close_failed',
            claimedTokenA: 0,
            claimedTokenB: 0,
            tokenASymbol: positionValue.tokenASymbol,
            tokenBSymbol: positionValue.tokenBSymbol,
            swapResults: [],
            manualActionRequired: true
          };
        }
      }

      throw txError;
    }

  } catch (error) {
    console.error(`    ❌ Failed to close position: ${error.message}`);

    return {
      success: false,
      reason: error.message,
      fullError: error,
      positionAddress: position.toBase58(),
      method: 'removeAllLiquidityAndClosePosition',
      claimedTokenA: 0,
      claimedTokenB: 0,
      tokenASymbol: positionValue.tokenASymbol,
      tokenBSymbol: positionValue.tokenBSymbol,
      swapResults: [],
      manualActionRequired: true
    };
  }
}

// === UTILITY FUNCTION TO GET CURRENT SLOT ===
async function getCurrentSlot() {
  try {
    const slot = await connection.getSlot();
    return new BN(slot);
  } catch (error) {
    console.log(`⚠️ Could not get current slot, using timestamp: ${error.message}`);
    return new BN(Math.floor(Date.now() / 1000));
  }
}

// === CLOSE ALL POSITIONS MAIN FUNCTION ===
async function closeAllPositions(options = {}) {
  const {
    dryRun = false,
    filterByMinValue = true,
    batchMode = true,
    confirmBeforeEach = false,
    autoSwapToSol = AUTO_SWAP_TO_SOL
  } = options;
  
  console.log("🚀 Close All Positions Script Started");
  console.log("=".repeat(50));
  console.log(`⏰ Started at: ${getCurrentTimestamp()}`);
  console.log(`💰 Wallet: ${wallet.publicKey.toBase58()}`);
  console.log(`🔧 Dry run: ${dryRun ? 'YES' : 'NO'}`);
  console.log(`💎 Min value filter: ${filterByMinValue ? 'YES' : 'NO'}`);
  console.log(`📦 Batch mode: ${batchMode ? 'YES' : 'NO'}`);
  console.log(`🔄 Auto swap to SOL: ${autoSwapToSol ? 'YES' : 'NO'}`);
  console.log("=".repeat(50));
  
  try {
    // Get all user positions
    console.log("📍 Step 1: Getting all user positions...");
    const userPositions = await cpAmm.getPositionsByUser(wallet.publicKey);
    console.log(`👤 Found ${userPositions.length} total positions`);
    
    if (userPositions.length === 0) {
      console.log("ℹ️ No positions found. Nothing to close.");
      return;
    }
    
    // Analyze all positions
    console.log("\n📍 Step 2: Analyzing positions...");
    const positionAnalyses = [];
    
    for (let i = 0; i < userPositions.length; i++) {
      console.log(`🔍 Analyzing position ${i + 1}/${userPositions.length}...`);
      const analysis = await analyzePosition(userPositions[i]);
      
      if (analysis) {
        positionAnalyses.push(analysis);
        
        const worth = analysis.isWorthClosing ? '✅ WORTH CLOSING' : '⚠️ SKIP (low value/locked)';
        console.log(`    ${analysis.tokenAMetadata.symbol}-${analysis.tokenBMetadata.symbol}: ${worth}`);
      }
      
      // Small delay to avoid rate limiting
      if (i < userPositions.length - 1) {
        await sleep(200);
      }
    }
    
    // Filter positions worth closing
    const positionsToClose = filterByMinValue 
      ? positionAnalyses.filter(p => p.isWorthClosing)
      : positionAnalyses.filter(p => !p.isLocked);
    
    console.log(`\n📊 Analysis Results:`);
    console.log(`📈 Total positions: ${positionAnalyses.length}`);
    console.log(`🎯 Worth closing: ${positionsToClose.length}`);
    console.log(`🔒 Locked positions: ${positionAnalyses.filter(p => p.isLocked).length}`);
    console.log(`💎 Low value positions: ${positionAnalyses.filter(p => !p.isWorthClosing && !p.isLocked).length}`);
    
    if (positionsToClose.length === 0) {
      console.log("ℹ️ No positions worth closing. Exiting.");
      return;
    }
    
    // Show positions to be closed
    console.log(`\n📋 Positions to close:`);
    positionsToClose.forEach((pos, i) => {
      console.log(`  ${i + 1}. ${pos.tokenAMetadata.symbol}-${pos.tokenBMetadata.symbol}: ${pos.positionValue.tokenA} ${pos.positionValue.tokenASymbol}, ${pos.positionValue.tokenB} ${pos.positionValue.tokenBSymbol}`);
    });
    
    if (dryRun) {
      console.log("\n🔍 DRY RUN MODE - No transactions will be sent");
      console.log("✅ Analysis complete. Use dryRun: false to execute.");
      return;
    }
    
    // Confirm before proceeding
    console.log(`\n⚠️ WARNING: About to close ${positionsToClose.length} positions!`);
    console.log("Press Ctrl+C to cancel or wait 10 seconds to continue...");
    await sleep(10000);
    
    // Close positions
    console.log("\n📍 Step 3: Closing positions...");
    const results = {
      success: [],
      failed: [],
      skipped: []
    };
    
    if (batchMode) {
      // Process in batches
      for (let i = 0; i < positionsToClose.length; i += BATCH_SIZE) {
        const batch = positionsToClose.slice(i, i + BATCH_SIZE);
        console.log(`\n📦 Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(positionsToClose.length / BATCH_SIZE)}`);
        
        for (let j = 0; j < batch.length; j++) {
          const positionAnalysis = batch[j];
          const globalIndex = i + j;
          
          if (confirmBeforeEach) {
            console.log(`\n❓ Close position ${globalIndex + 1}? (y/N)`);
            // In a real implementation, you'd wait for user input here
            // For now, we'll proceed automatically
          }
          
          const result = await closeSinglePosition(positionAnalysis, globalIndex, positionsToClose.length);
          
          if (result.success) {
            results.success.push(result);
          } else {
            if (result.reason === 'locked') {
              results.skipped.push(result);
            } else {
              results.failed.push(result);
            }
          }
          
          // Delay between transactions
          if (j < batch.length - 1) {
            await sleep(DELAY_BETWEEN_TRANSACTIONS);
          }
        }
        
        // Delay between batches
        if (i + BATCH_SIZE < positionsToClose.length) {
          console.log(`⏳ Waiting ${DELAY_BETWEEN_BATCHES / 1000}s before next batch...`);
          await sleep(DELAY_BETWEEN_BATCHES);
        }
      }
    } else {
      // Process one by one
      for (let i = 0; i < positionsToClose.length; i++) {
        const result = await closeSinglePosition(positionsToClose[i], i, positionsToClose.length);
        
        if (result.success) {
          results.success.push(result);
        } else {
          if (result.reason === 'locked') {
            results.skipped.push(result);
          } else {
            results.failed.push(result);
          }
        }
        
        if (i < positionsToClose.length - 1) {
          await sleep(DELAY_BETWEEN_TRANSACTIONS);
        }
      }
    }
    
    // Final summary
    console.log(`\n🎉 === FINAL SUMMARY ===`);
    console.log(`⏰ Completed at: ${getCurrentTimestamp()}`);
    console.log(`✅ Successfully closed: ${results.success.length}`);
    console.log(`❌ Failed to close: ${results.failed.length}`);
    console.log(`⚠️ Skipped (locked): ${results.skipped.length}`);
    
    if (results.success.length > 0) {
      console.log(`\n💰 Total claimed fees:`);
      const feesSummary = new Map();
      
      results.success.forEach(result => {
        if (result.claimedTokenA > 0) {
          const current = feesSummary.get(result.tokenASymbol) || 0;
          feesSummary.set(result.tokenASymbol, current + result.claimedTokenA);
        }
        if (result.claimedTokenB > 0) {
          const current = feesSummary.get(result.tokenBSymbol) || 0;
          feesSummary.set(result.tokenBSymbol, current + result.claimedTokenB);
        }
      });
      
      for (const [symbol, amount] of feesSummary.entries()) {
        console.log(`  ${symbol}: ${amount}`);
      }
      
      // Auto-swap summary - LENGKAPI BAGIAN YANG HILANG
      if (autoSwapToSol) {
        console.log(`\n🔄 Auto-swap summary:`);
        let totalSwaps = 0;
        let successfulSwaps = 0;
        let skippedSwaps = 0;
        let failedSwaps = 0;
        const swappedTokens = new Map();
        
        results.success.forEach(result => {
          if (result.swapResults && Array.isArray(result.swapResults)) {
            result.swapResults.forEach(swapResult => {
              totalSwaps++;
              if (swapResult.success && !swapResult.skipped) {
                successfulSwaps++;
                const current = swappedTokens.get(swapResult.tokenSymbol) || 0;
                swappedTokens.set(swapResult.tokenSymbol, current + (swapResult.amountSwapped || 0));
              } else if (swapResult.skipped) {
                skippedSwaps++;
              } else {
                failedSwaps++;
              }
            });
          }
        });
        
        console.log(`  📊 Total swap attempts: ${totalSwaps}`);
        console.log(`  ✅ Successful swaps: ${successfulSwaps}`);
        console.log(`  ⏭️ Skipped swaps: ${skippedSwaps}`);
        console.log(`  ❌ Failed swaps: ${failedSwaps}`);
        
        if (swappedTokens.size > 0) {
          console.log(`  💱 Tokens swapped to SOL (100% of balance):`);
          for (const [symbol, amount] of swappedTokens.entries()) {
            console.log(`    ${symbol}: ${amount.toFixed(6)} (100%)`);
          }
        } else if (totalSwaps === 0) {
          console.log(`  ℹ️ No tokens were swapped (all positions contained SOL)`);
        }
      }
    }
    
    console.log(`\n✅ Close all positions script completed!`);
    
  } catch (error) {
    console.error("💥 Script failed:", error.message);
    process.exit(1);
  }
}

// === INTERACTIVE FUNCTIONS ===
async function closePositionsByPool(poolAddress) {
  console.log(`🎯 Closing positions for pool: ${poolAddress}`);
  
  try {
    const userPositions = await cpAmm.getPositionsByUser(wallet.publicKey);
    const poolPositions = userPositions.filter(pos => 
      pos.positionState.pool.toBase58() === poolAddress
    );
    
    console.log(`📍 Found ${poolPositions.length} positions for this pool`);
    
    if (poolPositions.length === 0) {
      console.log("ℹ️ No positions found for this pool.");
      return;
    }
    
    for (let i = 0; i < poolPositions.length; i++) {
      const analysis = await analyzePosition(poolPositions[i]);
      if (analysis) {
        await closeSinglePosition(analysis, i, poolPositions.length);
        if (i < poolPositions.length - 1) {
          await sleep(DELAY_BETWEEN_TRANSACTIONS);
        }
      }
    }
    
    console.log(`✅ Completed closing ${poolPositions.length} positions for pool`);
    
  } catch (error) {
    console.error("❌ Error closing positions by pool:", error.message);
    throw error;
  }
}

async function closePositionsByToken(tokenSymbol) {
  console.log(`🎯 Closing positions containing token: ${tokenSymbol}`);
  
  try {
    const userPositions = await cpAmm.getPositionsByUser(wallet.publicKey);
    const tokenPositions = [];
    
    for (const userPosition of userPositions) {
      const analysis = await analyzePosition(userPosition);
      if (analysis && 
          (analysis.tokenAMetadata.symbol === tokenSymbol || 
           analysis.tokenBMetadata.symbol === tokenSymbol)) {
        tokenPositions.push(analysis);
      }
    }
    
    console.log(`📍 Found ${tokenPositions.length} positions containing ${tokenSymbol}`);
    
    if (tokenPositions.length === 0) {
      console.log(`ℹ️ No positions found containing token ${tokenSymbol}.`);
      return;
    }
    
    for (let i = 0; i < tokenPositions.length; i++) {
      await closeSinglePosition(tokenPositions[i], i, tokenPositions.length);
      if (i < tokenPositions.length - 1) {
        await sleep(DELAY_BETWEEN_TRANSACTIONS);
      }
    }
    
    console.log(`✅ Completed closing ${tokenPositions.length} positions containing ${tokenSymbol}`);
    
  } catch (error) {
    console.error("❌ Error closing positions by token:", error.message);
    throw error;
  }
}

// === BULK SWAP EXISTING TOKENS ===
async function swapAllTokensToSol() {
  console.log("🔄 Swapping all existing tokens to SOL...");
  console.log("=".repeat(50));
  
  try {
    // Get all token accounts for the wallet
    console.log("📍 Step 1: Getting all token accounts...");
    const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
      wallet.publicKey,
      { programId: TOKEN_PROGRAM_ID }
    );
    
    console.log(`📍 Found ${tokenAccounts.value.length} token accounts`);
    
    if (tokenAccounts.value.length === 0) {
      console.log("ℹ️ No token accounts found.");
      return [];
    }
    
    // Filter tokens worth swapping
    const tokensToSwap = [];
    
    for (let i = 0; i < tokenAccounts.value.length; i++) {
      const tokenAccount = tokenAccounts.value[i];
      const accountInfo = tokenAccount.account.data.parsed.info;
      const mint = new PublicKey(accountInfo.mint);
      const balance = accountInfo.tokenAmount.uiAmount;
      
      // Skip if SOL or zero balance or below minimum
      if (mint.toBase58() === SOL_MINT || balance === 0 || balance < MIN_SWAP_AMOUNT) {
        continue;
      }
      
      try {
        const metadata = await getTokenMetadata(mint);
        tokensToSwap.push({
          mint,
          symbol: metadata.symbol,
          decimals: metadata.decimals,
          balance: balance,
          account: tokenAccount.pubkey
        });
        
        console.log(`  ✅ Found swappable token: ${metadata.symbol} (${balance})`);
        
      } catch (error) {
        console.error(`  ⚠️ Error getting metadata for ${mint.toBase58()}: ${error.message}`);
      }
      
      // Small delay to avoid rate limiting
      await sleep(100);
    }
    
    console.log(`\n📊 Analysis Results:`);
    console.log(`📈 Total token accounts: ${tokenAccounts.value.length}`);
    console.log(`🎯 Tokens worth swapping: ${tokensToSwap.length}`);
    
    if (tokensToSwap.length === 0) {
      console.log("ℹ️ No tokens worth swapping found.");
      return [];
    }
    
    // Show tokens to be swapped
    console.log(`\n📋 Tokens to swap:`);
    tokensToSwap.forEach((token, i) => {
      console.log(`  ${i + 1}. ${token.symbol}: ${token.balance}`);
    });
    
    // Confirm before proceeding
    console.log(`\n⚠️ WARNING: About to swap ${tokensToSwap.length} tokens to SOL!`);
    console.log("Press Ctrl+C to cancel or wait 5 seconds to continue...");
    await sleep(5000);
    
    // Start swapping
    console.log("\n📍 Step 2: Swapping tokens...");
    const swapResults = [];
    let successCount = 0;
    let failedCount = 0;
    let skippedCount = 0;
    
    for (let i = 0; i < tokensToSwap.length; i++) {
      const token = tokensToSwap[i];
      
      console.log(`\n[${i + 1}/${tokensToSwap.length}] 💱 Swapping: ${token.symbol}`);
      
      try {
        const swapResult = await swapTokensToSol(
          token.mint,
          token.symbol,
          token.decimals,
          MIN_SWAP_AMOUNT
        );
        
        swapResults.push({
          ...swapResult,
          originalBalance: token.balance
        });
        
        if (swapResult.success && !swapResult.skipped) {
          successCount++;
          console.log(`  ✅ Successfully swapped ${token.symbol}`);
        } else if (swapResult.skipped) {
          skippedCount++;
          console.log(`  ⏭️ Skipped: ${token.symbol}`);
        } else {
          failedCount++;
          console.log(`  ❌ Failed: ${token.symbol} - ${swapResult.error}`);
        }
        
      } catch (error) {
        failedCount++;
        console.error(`  ❌ Error swapping ${token.symbol}: ${error.message}`);
        swapResults.push({
          success: false,
          error: error.message,
          tokenSymbol: token.symbol,
          originalBalance: token.balance
        });
      }
      
      // Delay between swaps to avoid rate limiting
      if (i < tokensToSwap.length - 1) {
        await sleep(1500);
      }
    }
    
    // Final summary
    console.log(`\n🎉 === BULK SWAP SUMMARY ===`);
    console.log(`⏰ Completed at: ${getCurrentTimestamp()}`);
    console.log(`📊 Total tokens processed: ${tokensToSwap.length}`);
    console.log(`✅ Successfully swapped: ${successCount}`);
    console.log(`⏭️ Skipped: ${skippedCount}`);
    console.log(`❌ Failed: ${failedCount}`);
    
    if (successCount > 0) {
      console.log(`\n💱 Successfully swapped tokens:`);
      const successfulSwaps = swapResults.filter(r => r.success && !r.skipped);
      successfulSwaps.forEach(swap => {
        console.log(`  ${swap.tokenSymbol}: ${swap.amountSwapped} (100%)`);
      });
    }
    
    if (failedCount > 0) {
      console.log(`\n❌ Failed swaps:`);
      const failedSwaps = swapResults.filter(r => !r.success);
      failedSwaps.forEach(swap => {
        console.log(`  ${swap.tokenSymbol}: ${swap.error || 'Unknown error'}`);
      });
    }
    
    console.log(`\n✅ Bulk swap completed!`);
    return swapResults;
    
  } catch (error) {
    console.error("💥 Bulk swap failed:", error.message);
    throw error;
  }
}

// === UTILITY FUNCTIONS ===
async function showWalletTokens() {
  console.log("📊 Wallet Token Summary");
  console.log("=".repeat(50));
  
  try {
    // Get SOL balance
    const solBalance = await connection.getBalance(wallet.publicKey);
    console.log(`💰 SOL Balance: ${solBalance / 1e9} SOL\n`);
    
    // Get token accounts
    const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
      wallet.publicKey,
      { programId: TOKEN_PROGRAM_ID }
    );
    
    console.log(`📍 Found ${tokenAccounts.value.length} token accounts:\n`);
    
    let totalValue = 0;
    const tokens = [];
    
    for (let i = 0; i < tokenAccounts.value.length; i++) {
      const tokenAccount = tokenAccounts.value[i];
      const accountInfo = tokenAccount.account.data.parsed.info;
      const mint = new PublicKey(accountInfo.mint);
      const balance = accountInfo.tokenAmount.uiAmount;
      
      if (balance > 0) {
        try {
          const metadata = await getTokenMetadata(mint);
          tokens.push({
            symbol: metadata.symbol,
            balance: balance,
            mint: mint.toBase58(),
            decimals: metadata.decimals
          });
          
          console.log(`  ${i + 1}. ${metadata.symbol}: ${balance}`);
          
        } catch (error) {
          console.log(`  ${i + 1}. Unknown (${mint.toBase58().slice(0, 8)}...): ${balance}`);
        }
      }
      
      await sleep(100); // Rate limiting
    }
    
    console.log(`\n📈 Total tokens with balance: ${tokens.length}`);
    console.log(`💎 Non-SOL tokens: ${tokens.filter(t => t.symbol !== 'SOL').length}`);
    
    return tokens;
    
  } catch (error) {
    console.error("❌ Error showing wallet tokens:", error.message);
    throw error;
  }
}

// === SCRIPT EXECUTION ===
async function main() {
  console.log(`🚀 Remove Positions Script Started`);
  console.log(`⏰ Started at: ${getCurrentTimestamp()}`);
  console.log(`🔍 Debug mode: Checking prerequisites...`);
  
  // Enhanced prerequisite check
  console.log(`📋 PRIVATE_KEY_BS58 status: ${PRIVATE_KEY_BS58 ? '✅ Provided' : '❌ EMPTY'}`);
  
  if (!PRIVATE_KEY_BS58 || PRIVATE_KEY_BS58.trim() === '') {
    console.error("❌ ERROR: PRIVATE_KEY_BS58 tidak boleh kosong!");
    console.log("\n🔧 CARA FIX:");
    console.log("1. Buka file remove.js");
    console.log("2. Cari baris: const PRIVATE_KEY_BS58 = \"\";");
    console.log("3. Isi dengan private key dari Phantom wallet");
    console.log("4. Contoh: const PRIVATE_KEY_BS58 = \"5Kb8kLf9zgWQnogidDA76MzPL6TsZZY36hWXMssSzNydYXYB9KF....\";");
    console.log("\n⚠️  JANGAN SHARE PRIVATE KEY KE SIAPAPUN!");
    process.exit(1);
  }

  // Test wallet creation SETELAH check private key
  try {
    console.log(`🔑 Testing wallet creation...`);
    wallet = Keypair.fromSecretKey(bs58.decode(PRIVATE_KEY_BS58)); // BUAT WALLET DI SINI
    console.log(`✅ Wallet loaded successfully`);
    console.log(`💰 Wallet Address: ${wallet.publicKey.toBase58()}`);
  } catch (error) {
    console.error("❌ ERROR: Invalid private key format!");
    console.log("\n🔧 CARA FIX:");
    console.log("1. Pastikan private key dalam format base58");
    console.log("2. Copy private key dari Phantom wallet");
    console.log("3. Jangan include spasi atau karakter lain");
    console.log(`\n📋 Error detail: ${error.message}`);
    process.exit(1);
  }
  
  console.log(`🚀 Remove Positions Script Started`);
  console.log(`⏰ Started at: ${getCurrentTimestamp()}`);
  console.log(`💰 Wallet: ${wallet.publicKey.toBase58()}`);
  
  const solBalance = await connection.getBalance(wallet.publicKey);
  console.log(`💰 Current SOL Balance: ${solBalance / 1e9} SOL\n`);
  
  // Parse command line arguments
  const args = process.argv.slice(2);
  const mode = args[0] || 'help';
  const target = args[1];
  
  try {
    switch (mode) {
      case 'all':
        await closeAllPositions({
          dryRun: false,
          filterByMinValue: true,
          batchMode: true,
          confirmBeforeEach: false,
          autoSwapToSol: true
        });
        break;
        
      case 'dry':
        await closeAllPositions({
          dryRun: true,
          filterByMinValue: true,
          batchMode: true,
          autoSwapToSol: true
        });
        break;

      case 'no-swap':
        await closeAllPositions({
          dryRun: false,
          filterByMinValue: true,
          batchMode: true,
          confirmBeforeEach: false,
          autoSwapToSol: false
        });
        break;

      case 'swap-only':
        await swapAllTokensToSol();
        break;

      case 'show-tokens':
        await showWalletTokens();
        break;
        
      case 'pool':
        if (!target) {
          console.error("❌ Pool address required for pool mode");
          console.log("Usage: node remove.js pool <pool_address>");
          process.exit(1);
        }
        await closePositionsByPool(target);
        break;
        
      case 'token':
        if (!target) {
          console.error("❌ Token symbol required for token mode");
          console.log("Usage: node remove.js token <token_symbol>");
          process.exit(1);
        }
        await closePositionsByToken(target);
        break;

      case 'help':
      default:
        console.log("🔧 Available Commands:");
        console.log("=".repeat(50));
        console.log("  node remove.js all               # Close all positions + auto swap to SOL");
        console.log("  node remove.js dry               # Dry run - analyze only");
        console.log("  node remove.js no-swap           # Close positions without auto swap");
        console.log("  node remove.js swap-only         # Only swap existing tokens to SOL");
        console.log("  node remove.js show-tokens       # Show all wallet tokens");
        console.log("  node remove.js pool <address>    # Close positions for specific pool");
        console.log("  node remove.js token <symbol>    # Close positions containing token");
        console.log("  node remove.js help              # Show this help");
        console.log("\n📋 Examples:");
        console.log("  node remove.js dry                                    # Safe analysis first");
        console.log("  node remove.js all                                    # Full cleanup");
        console.log("  node remove.js token BONK                             # Close BONK positions");
        console.log("  node remove.js pool 9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM  # Specific pool");
        console.log("\n⚠️  Always run 'dry' mode first to see what will be processed!");
        break;
    }
    
  } catch (error) {
    console.error("💥 Script execution failed:", error.message);
    if (error.stack) {
      console.error("Stack trace:", error.stack);
    }
    process.exit(1);
  }
}

// Start the script only if run directly
  main().catch(error => {
    console.error("💥 Fatal error:", error.message);
    process.exit(1);
  });


// Export functions for use in other modules
export { 
  closeAllPositions, 
  closeSinglePosition, 
  closePositionsByPool, 
  closePositionsByToken,
  analyzePosition,
  swapAllTokensToSol,
  showWalletTokens,
  autoSwap,
  getTokenBalance,
  swapTokensToSol
};
