#!/usr/bin/env python3
"""
Yahoo Finance Market Data Updater for GitHub Actions
Updates market data for Yahoo-sourced TSX-V companies
Runs on schedule: Intraday (5 min), Daily (6 AM EST), Weekly (Sunday 6 PM EST)
"""

import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
from datetime import datetime
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

# All 589 Yahoo Finance TSX Venture companies
YAHOO_COMPANIES = [
    'ARH.V', 'ALTU.V', 'ATLE.V', 'BGE.V', 'BLU.V', 'SPI.V', 'CCEC.V', 'CTA.V', 'CFY.V', 'CEQ.V', 'EW.V', 'FHR.V', 'GX.V', 'HAM.V', 'HPL.V', 'LTX.V', 'LNGE.V', 
    'LTC.V', 'LCX.V', 'MAH.V', 'MCF.V', 'NZ.V', 'NGY.V', 'ORC-B.V', 'PFC.V', 'PCQ.V', 'VRY.V', 'PTC.V', 'STMP.V', 'SKK.V', 'TPC.V', 'TGH.V', 'TTG.V', 'VTX.V', 'WGT.V', 
    'XL.V', 'ADYA.V', 'AIVC.V', 'GPUS.V', 'AUUA.V', 'SAT.V', 'BRN-PA.V', 'BCF.V', 'CSOC-A.V', 'CSOC-B.V', 'CYF.V', 'CAI.V', 'CAG.V', 'FFP.V', 'CTH.V', 'DTEA.V', 
    'DWS.V', 'DLC.V', 'DPF.V', 'ELM.V', 'ESBL.V', 'FDI.V', 'FCA-U.V', 'FW.V', 'FORT.V', 'FA.V', 'FP.V', 'FRSH.V', 'GRF.V', 'GUF.V', 'HFC.V', 'HILL.V', 'ICRS.V', 'IMPT.V', 
    'IGP.V', 'IPD.V', 'KDA.V', 'VAND.V', 'MNC.V', 'MNX.V', 'MAP.V', 'ALFA-UN.V', 'MAR-UN.V', 'MIVO.V', 'MNLX.V', 'NXG.V', 'NXLV.V', 'NFD-A.V', 'OC.V', 'OML.V', 
    'PALI.V', 'PVF-UN.V', 'PRH.V', 'PTFY.V', 'POOL.V', 'BRED.V', 'RRR-UN.V', 'RGI.V', 'RFX.V', 'RPP.V', 'RUM.V', 'RMB.V', 'HASH.V', 'SSX.V', 'SNI-PA.V', 'SQG.V', 'SSA.V', 
    'SPP.V', 'SCPT-U.V', 'SURF-A.V', 'SGE.V', 'SUGR.V', 'SRES.V', 'YAY.V', 'TORR.V', 'TRBR.V', 'GYM.V', 'USS.V', 'UIG.V', 'WI.V', 'WP.V', 'YEG.V', 'EFF.V', 'ABA.V', 
    'ABM.V', 'ACDC.V', 'ADE.V', 'ADY.V', 'ALTN.V', 'AERO.V', 'AFR.V', 'AML.V', 'AEMC.V', 'ALM.V', 'AORO.V', 'ATI.V', 'AVX.V', 'ALT.V', 'AWM.V', 'ANTL.V', 'APX.V', 'APMI.V', 
    'ARJN.V', 'ACS.V', 'AWX.V', 'AGAG.V', 'ASL.V', 'RBZ.V', 'ATOM.V', 'AUGC.V', 'AUQ.V', 'RES.V', 'ARL.V', 'AGLD.V', 'AVR.V', 'AXO.V', 'AZR.V', 'AZT.V', 'BLDS.V', 'BGS.V', 
    'FIND.V', 'BAT.V', 'BMV.V', 'BMR.V', 'BM.V', 'B.V', 'BGF.V', 'BFM.V', 'BST.V', 'BIGT.V', 'BMM.V', 'BAG.V', 'BOCA.V', 'BOL.V', 'BNZ.V', 'BOGO.V', 'BONE.V', 'BRON.V', 
    'ZLTO.V', 'BGD.V', 'BWR.V', 'CONE.V', 'CCMI.V', 'CAN.V', 'CAF.V', 'CLV.V', 'CDA.V', 'CRB.V', 'CGD.V', 'CPI.V', 'RUSH.V', 'CASA.V', 'CCD.V', 'CTG.V', 'CIO.V', 'CERT.V', 
    'CDPR.V', 'CBA.V', 'CBG.V', 'TRAN.V', 'CTV.V', 'CZZ.V', 'CLUS.V', 'CLIC.V', 'CVB.V', 'CQR.V', 'CLM.V', 'CRD.V', 'CUEX.V', 'CNCO.V', 'COSA.V', 'CSG.V', 'CYG.V', 'DTWO.V', 
    'DGC.V', 'DFR.V', 'DNO.V', 'DHR.V', 'DCY.V', 'DLP.V', 'DMCU.V', 'DRY.V', 'DYG.V', 'ETU.V', 'EDM.V', 'EGR.V', 'EML.V', 'ELY.V', 'EP.V', 'ENEV.V', 'EAU.V', 'EGM.V', 
    'EON.V', 'REE.V', 'EOX.V', 'EVX.V', 'EXN.V', 'EXG.V', 'FAIR.V', 'FLCN.V', 'FMN.V', 'FINX.V', 'FAS.V', 'FAN.V', 'FCI.V', 'FNM.V', 'FTZ.V', 'FEX.V', 'FKM.V', 'FTJ.V', 
    'FMT.V', 'FMM.V', 'FTUR.V', 'GETT.V', 'GRI.V', 'GEN.V', 'GTC.V', 'GGL.V', 'GGX.V', 'GLAD.V', 'CUCU.V', 'GXX.V', 'HART.V', 'GLB.V', 'AUX.V', 'GDP.V', 'GLDN.V', 'GDX.V', 
    'GOFL.V', 'GGA.V', 'GHL.V', 'GSTM.V', 'GPM.V', 'GPAC.V', 'GPS.V', 'GMI.V', 'VGN.V', 'GRL.V', 'FIN.V', 'GSPR.V', 'GT.V', 'GUN.V', 'GYA.V', 'HPM.V', 'HANS.V', 'HPY.V', 
    'HAWK.V', 'HAY.V', 'HHH.V', 'HWY.V', 'ESPN.V', 'HLU.V', 'IMC.V', 'IDEX.V', 'IRI.V', 'INFD.V', 'INFM.V', 'INFI.V', 'MINE.V', 'ICON.V', 'IMM.V', 'IZZ.V', 'IRO.V', 'INTR.V', 
    'IZN.V', 'JTWO.V', 'JADE.V', 'JDN.V', 'JHC.V', 'JZR.V', 'KAPA.V', 'KIB.V', 'KCP.V', 'KNG.V', 'KLDC.V', 'KSM.V', 'KRI.V', 'KRY.V', 'LAB.V', 'LG.V', 'LWR.V', 'LMS.V', 
    'LEGY.V', 'LIB.V', 'LMG.V', 'ROAR.V', 'LOD.V', 'LPK.V', 'LUXR.V', 'MT.V', 'MDM.V', 'MGMA.V', 'MGI.V', 'MARV.V', 'MCM-A.V', 'MFL.V', 'MAX.V', 'MLO.V', 'MSC.V', 'MMM.V', 
    'MHI.V', 'MINK.V', 'MSG.V', 'MOG.V', 'MON.V', 'MNRG.V', 'MOO.V', 'MCC.V', 'MUN.V', 'NTX.V', 'NVLH.V', 'NED.V', 'ENRG.V', 'NTB.V', 'NEXM.V', 'NICN.V', 'NCP.V', 'NIO.V', 
    'NOAL.V', 'NOBL.V', 'NVT.V', 'NSU.V', 'NL.V', 'NMC.V', 'FEO.V', 'OLV.V', 'ONYX.V', 'OPTG.V', 'OOR.V', 'ORCL.V', 'PBM.V', 'PPM.V', 'PDQ.V', 'PGDC.V', 'PAT.V', 'PEGA.V', 
    'PINN.V', 'PJX.V', 'PLA.V', 'PWRO.V', 'PPX.V', 'HDRO.V', 'PMX.V', 'PGX.V', 'PHD.V', 'PTX.V', 'PWH.V', 'QTWO.V', 'QCX.V', 'QGR.V', 'QRO.V', 'LEAP.V', 'Q.V', 'QURI.V', 
    'RAK.V', 'RMO.V', 'RAMP.V', 'RNCH.V', 'RTH.V', 'REC.V', 'RGC.V', 'RAGE.V', 'RSM.V', 'REVX.V', 'RYE.V', 'RMD.V', 'RMI.V', 'RLYG.V', 'RJX-A.V', 'RCT.V', 'RTE.V', 'RTM.V', 
    'SAGA.V', 'SAGE.V', 'SLG.V', 'SCD.V', 'SCY.V', 'SAF.V', 'SGZ.V', 'SEND.V', 'SHRP.V', 'SIEN.V', 'SVG.V', 'SPD.V', 'AGA.V', 'SXL.V', 'SMRV.V', 'SDCU.V', 'SGO.V', 'SAO.V', 
    'SPMC.V', 'SGQ.V', 'SML.V', 'SRQ.V', 'SRC.V', 'STUD.V', 'STRM.V', 'SMD.V', 'SR.V', 'SDR.V', 'STUV.V', 'PEAK.V', 'SUI.V', 'SYG.V', 'TWO.V', 'RARE.V', 'TRO.V', 'TKU.V', 
    'TORA.V', 'TES.V', 'TGX.V', 'MAC.V', 'BIRD.V', 'TIN.V', 'TORC.V', 'TTS.V', 'TBLL.V', 'TGC.V', 'TBK.V', 'TCO.V', 'TFM.V', 'TRS.V', 'TRBC.V', 'TG.V', 'TR.V', 'TSD.V', 
    'URZ.V', 'VMXX.V', 'VAX.V', 'VCV.V', 'VLX.V', 'VMET.V', 'VLD.V', 'VIZ.V', 'VCT.V', 'VMS.V', 'WLR.V', 'WRI.V', 'WGF.V', 'WPG.V', 'WGLD.V', 'WMS.V', 'WKG.V', 'WMK.V', 
    'WSK.V', 'GIG.V', 'XPLR.V', 'XXIX.V', 'ZBNI.V', 'ZNX.V', 'ZAU.V', 'ZON.V', 'BQE.V', 'KLX.V', 'CTEK.V', 'FWTC.V', 'FCLI.V', 'HEMP.V', 'IBAT.V', 'COO.V', 'SPRQ.V', 'SUN.V', 
    'WEB.V', 'AWI.V', 'AGET.V', 'AISX.V', 'AMT.V', 'ARGH.V', 'AST.V', 'BECN.V', 'SWAN.V', 'MATE.V', 'BTV.V', 'BILD.V', 'WPR.V', 'CCDS.V', 'WAGR.V', 'CNS.V', 'CUB.V', 'CYBE.V', 
    'DAR.V', 'MKT.V', 'DFSC.V', 'DGX.V', 'ENA.V', 'EPF.V', 'FTRC.V', 'GOK.V', 'GOOD.V', 'HIDE.V', 'ICGH.V', 'ID.V', 'INEO.V', 'INX.V', 'INIK.V', 'JJ.V', 'JTC.V', 'KDOZ.V', 
    'DDD.V', 'MIM.V', 'MIT.V', 'BET.V', 'PVIS.V', 'KEEK.V', 'PTEC.V', 'XBOT.V', 'SBIO.V', 'SPZ.V', 'THP.V', 'TTGI.V', 'VIP.V', 'VITA.V', 'YTY.V', 'XCYT.V', 'XTAO-U.V', 'ZMA.V', 
    'FRED.V', 'AJA.V', 'ARC.V', 'BGA.V', 'EKG.V', 'CSPN.V', 'CHER.V', 'CNVI.V', 'CYTO.V', 'GSD.V', 'OKAI.V', 'EVMT.V', 'FREQ.V', 'GENX.V', 'WOLF.V', 'NURS.V', 'IDL.V', 'IOT.V', 
    'KOVO.V', 'JUMP.V', 'LSL.V', 'MDX.V', 'NSCI.V', 'NAV.V', 'NPTH.V', 'NGMD.V', 'NRX.V', 'PCRX.V', 'QPT.V', 'RKV.V', 'SHRX.V', 'SBM.V', 'TTI.V', 'VPI.V', 'VVTM.V', 'WAVE.V', 
    'ZCT.V', 'ZYUS.V', 'HPSS.CN', 'KLN.CN', 'NUE.CN', 'RGEN.CN', 'RGX.CN', 'APKI.CN', 'ADPT.CN', 'BVOF-A.CN', 'BVOF-B.CN', 'BKTS.CN',
'BKS.CN', 'BRCH.CN', 'BHCC.CN', 'BTC.PR.A.CN', 'CISC.CN', 'CLDV.CN', 'CODE-X.CN', 'DATT.CN', 'GCA-X.CN', 'HYLQ.CN',
'MESC.CN', 'METX.CN', 'MBAI.CN', 'MOSS.CN', 'MYCO.CN', 'NURL.CN', 'VFI-X.CN', 'ORNG.CN', 'LOAN.CN', 'PLTH-WT.CN',
'REK-U.CN', 'RI.CN', 'SPLY.CN', 'SKY.CN', 'SNDL.CN', 'CRIT.CN', 'TLP-UN.CN', 'AITT.CN', 'TWOH-X.CN', 'LFG.CN',
'URB-A.CN', 'VICE.CN', 'WSM-X.CN', 'ZOG-X.CN', 'AAWH-U.CN', 'CURE-X.CN', 'NIC.CN', 'DCNN.CN', 'FNT-U.CN', 'FGH.CN',
'HERB.CN', 'JOLT.CN', 'VTAL.CN', 'MBIO.CN', 'MTLC.CN', 'AIAI.CN', 'MUSL.CN', 'QNTM.CN', 'TRUL-NTU.CN', 'VREO.CN',
'JJJ.CN', 'AMQ.CN', 'ADDY.CN', 'ADON.CN', 'AUEX.CN', 'AFF.CN', 'ACM.CN', 'NUKE.CN', 'ACRE.CN', 'KCLI.CN',
'USLI.CN', 'TUNG.CN', 'AWCM.CN', 'ANDC.CN', 'ANT.CN', 'ATMY.CN', 'APXC.CN', 'ARGL.CN', 'ARMY.CN', 'AUMC.CN',
'AGC.CN', 'AVM-X.CN', 'AVE.CN', 'AA.CN', 'BAR.CN', 'BATX.CN', 'BYRG.CN', 'BGX.CN', 'BBRD.CN', 'BLST.CN',
'BGLD.CN', 'BRS.CN', 'CSQ.CN', 'BRAZ.CN', 'CASC.CN', 'CMET.CN', 'CMP.CN', 'COMT.CN', 'CQX.CN', 'CSR.CN',
'CUH.CN', 'CRTL.CN', 'CRPC.CN', 'CRVC-X.CN', 'CUPA.CN', 'BATT.CN', 'DEMC.CN', 'EPR.CN', 'ER.CN', 'EAGL.CN',
'WISE.CN', 'EVM.CN', 'PHOS.CN', 'FRG.CN', 'FOMO.CN', 'FREE.CN', 'BOOM.CN', 'GMC.CN', 'BFG.CN', 'BFG-WT.B.CN',
'BFG-WTA.CN', 'GSTR.CN', 'MONI.CN', 'GURN.CN', 'GC.CN', 'GDN.CN', 'GCC.CN', 'GLDR.CN', 'GFT.CN', 'GRBM.CN',
'GXP.CN', 'HARD.CN', 'HERC.CN', 'HZ.CN', 'HLND.CN', 'HRK.CN', 'ISP.CN', 'INTG.CN', 'KING.CN', 'KOG.CN',
'KBX.CN', 'LAI-X.CN', 'LFNT.CN', 'LIBR.CN', 'LINE.CN', 'LEO.CN', 'LIVE.CN', 'MBL.CN', 'KENY.CN', 'MAXM.CN',
'MLM.CN', 'MERC.CN', 'METL.CN', 'MSM.CN', 'MMET.CN', 'MILI.CN', 'ROAD.CN', 'MY.CN', 'MEC.CN', 'M.CN',
'NATB.CN', 'NTMC.CN', 'NOP.CN', 'EATH.CN', 'PATH.CN', 'NEXX.CN', 'NXU-X.CN', 'NEXU.CN', 'NIX.CN', 'NVPC.CN',
'NUKV.CN', 'OMGA.CN', 'PSIL.CN', 'PURR.CN', 'PGA.CN', 'PGR.CN', 'PLTO.CN', 'PMAX.CN', 'PRCG.CN', 'PRNC.CN',
'GRUV.CN', 'QMET.CN', 'TIM-X.CN', 'QB.CN', 'QIMC.CN', 'QREE.CN', 'QQQ.CN', 'REDC.CN', 'RUU.CN', 'RECE.CN',
'RSG.CN', 'SK.CN', 'RB.CN', 'SALI.CN', 'SCU.CN', 'SEAG-X.CN', 'SEEM.CN', 'SHOW.CN', 'SI.CN', 'SRS.CN',
'SPRK.CN', 'SRAN.CN', 'STCU.CN', 'SLO.CN', 'STMN.CN', 'SUU.CN', 'SUR.CN', 'TMIN.CN', 'TCEC.CN', 'CACR.CN',
'CACR-A.CN', 'TRM.CN', 'TONE.CN', 'UE.CN', 'USCM.CN', 'UUU.CN', 'VRDN.CN', 'VLTA.CN', 'VRTX.CN', 'WSR.CN',
'XRI.CN', 'YMC.CN', 'ZEUS.CN', 'AETH.CN', 'ALEN.U.CN', 'ANON.CN', 'BRH.CN', 'BSKY.CN', 'TNJ.CN', 'BPAI.CN',
'CLTE.CN', 'CTTT.CN', 'DWTZ.CN', 'DTR.CN', 'KAS.CN', 'PLUG.CN', 'ENRT.CN', 'XBLK.X.CN', 'FDM.X.CN', 'FNDX.CN',
'WERX.CN', 'AICO.CN', 'AIG.CN', 'ROBO.CN', 'HYPE.CN', 'ISTK.CN', 'JIVA.CN', 'LITS.CN', 'METG.CN', 'LABZ.CN',
'NEWS.CN', 'NXT.CN', 'PXE.CN', 'STIF.CN', 'PLAS.CN', 'AIDR.CN', 'SHLF.CN', 'SPAI.CN', 'SPTZ.CN', 'SBTC.CN',
'STKT.CN', 'QBTQ.CN', 'SYAI.CN', 'TGGL.CN', 'TWEL.CN', 'WRUN.CN', 'WIN.CN', 'WISR.CN', 'VRAI.CN', 'UGH.CN'
]

# Firebase configuration (from environment variable)
FIREBASE_PROJECT_ID = 'canada-stocks-3c74f'

# ============================================================================
# INITIALIZATION
# ============================================================================

def initialize_firebase():
    """Initialize Firebase with credentials from environment"""
    if not firebase_admin._apps:
        cred_json = os.environ.get('FIREBASE_CREDENTIALS')
        if not cred_json:
            raise ValueError("FIREBASE_CREDENTIALS environment variable not set")
        
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    
    return firestore.client()

# ============================================================================
# UPDATE FUNCTIONS
# ============================================================================

def update_intraday_data(db):
    """
    Update real-time market data (every 5 minutes during market hours)
    Fields: Price, Volume, DayHigh, DayLow, Change, ChangePercent, Open
    """
    print(f"\n{'='*70}")
    print("INTRADAY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            # Fetch data from Yahoo Finance
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Extract ticker without suffix
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare realtime data
            realtime_data = {
                'price': info.get('currentPrice') or info.get('regularMarketPrice'),
                'volume': info.get('volume') or info.get('regularMarketVolume'),
                'dayHigh': info.get('dayHigh') or info.get('regularMarketDayHigh'),
                'dayLow': info.get('dayLow') or info.get('regularMarketDayLow'),
                'open': info.get('open') or info.get('regularMarketOpen'),
                'change': info.get('regularMarketChange'),
                'changePercent': info.get('regularMarketChangePercent')
            }
            
            # Also create flat fields for backward compatibility
            flat_data = {
                'Price': realtime_data['price'],
                'Volume': realtime_data['volume'],
                'DayHigh': realtime_data['dayHigh'],
                'DayLow': realtime_data['dayLow'],
                'Open': realtime_data['open'],
                'Change': realtime_data['change'],
                'ChangePercent': realtime_data['changePercent']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'realtime': realtime_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"INTRADAY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

def update_daily_data(db):
    """
    Update daily market data (once at 6:00 AM EST)
    Fields: MarketCap, PE, PreviousClose, 52WeekHigh, 52WeekLow, Beta, AvgVolume
    """
    print(f"\n{'='*70}")
    print("DAILY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare daily data
            daily_data = {
                'marketCap': info.get('marketCap'),
                'peRatio': info.get('trailingPE') or info.get('forwardPE'),
                'previousClose': info.get('previousClose') or info.get('regularMarketPreviousClose'),
                'week52High': info.get('fiftyTwoWeekHigh'),
                'week52Low': info.get('fiftyTwoWeekLow'),
                'beta': info.get('beta'),
                'avgVolume': info.get('averageVolume') or info.get('averageVolume10days')
            }
            
            # Flat fields for backward compatibility
            flat_data = {
                'MarketCap': daily_data['marketCap'],
                'PE': daily_data['peRatio'],
                'PreviousClose': daily_data['previousClose'],
                '52WeekHigh': daily_data['week52High'],
                '52WeekLow': daily_data['week52Low'],
                'Beta': daily_data['beta'],
                'AvgVolume': daily_data['avgVolume']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'daily': daily_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"DAILY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

def update_weekly_data(db):
    """
    Update weekly market data (Sunday 6:00 PM EST)
    Fields: SharesOutstanding, EPS, Float
    """
    print(f"\n{'='*70}")
    print("WEEKLY UPDATE - Yahoo Finance Companies")
    print(f"{'='*70}")
    print(f"Updating {len(YAHOO_COMPANIES)} companies...")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(YAHOO_COMPANIES, 1):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            clean_ticker = ticker.replace('.V', '').replace('.TO', '').replace('.CN', '')
            
            # Prepare monthly/weekly data
            monthly_data = {
                'sharesOutstanding': info.get('sharesOutstanding'),
                'eps': info.get('trailingEps') or info.get('forwardEps'),
                'float': info.get('floatShares')
            }
            
            # Flat fields for backward compatibility
            flat_data = {
                'SharesOutstanding': monthly_data['sharesOutstanding'],
                'EPS': monthly_data['eps'],
                'Float': monthly_data['float']
            }
            
            # Update Firebase
            db.collection('market_data').document(clean_ticker).set({
                'ticker': clean_ticker,
                'fullTicker': ticker,
                'monthly': monthly_data,
                **flat_data,
                'lastUpdated': firestore.SERVER_TIMESTAMP,
                'dataSource': 'Yahoo Finance'
            }, merge=True)
            
            success_count += 1
            
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(YAHOO_COMPANIES)} ({success_count} success, {fail_count} failed)")
            
            time.sleep(0.1)
            
        except Exception as e:
            fail_count += 1
            print(f"  Error updating {ticker}: {str(e)}")
    
    print(f"\n{'='*70}")
    print(f"WEEKLY UPDATE COMPLETE")
    print(f"  Success: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Determine update type from command line argument
    update_type = sys.argv[1] if len(sys.argv) > 1 else 'intraday'
    
    print(f"\n{'='*70}")
    print(f"YAHOO FINANCE MARKET DATA UPDATER")
    print(f"Update Type: {update_type.upper()}")
    print(f"{'='*70}\n")
    
    try:
        # Initialize Firebase
        db = initialize_firebase()
        
        # Run appropriate update
        if update_type == 'intraday':
            update_intraday_data(db)
        elif update_type == 'daily':
            update_daily_data(db)
        elif update_type == 'weekly':
            update_weekly_data(db)
        else:
            print(f"Unknown update type: {update_type}")
            print("Valid types: intraday, daily, weekly")
            sys.exit(1)
        
        print("\nUpdate completed successfully!")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
