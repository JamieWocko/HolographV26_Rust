use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::core::state::{AppState, OnlineUser};
use crate::db::db::Database;
use crate::encoding::jeax_encoding::{
    PacketBuffer, decode_b64, decode_vl64, encode_vl64, legacy_frame,
};
use crate::games::game_player::GamePlayer;
use crate::managers::{
    catalogue_manager, event_manager, rank_manager, recycler_manager, room_manager,
    sound_machine_manager, staff_manager, string_manager, user_manager,
};
use crate::messenger::virtual_messenger;
use crate::virtuals::rooms::virtual_room::VirtualRoom;
use crate::virtuals::users::virtual_room_user::VirtualRoomUser;
use crate::virtuals::users::virtual_song_editor::VirtualSongEditor;

const LOGIN_HANDSHAKE: &str = "DAQBHIIIKHJIPAIQAdd-MM-yyyy\u{2}SAHPB/client\u{2}QBHIJWVVVSNKQCFUBJASMSLKUUOJCOLJQPNSBIRSVQBRXZQOTGPMNJIHLVJCRRULBLUO";
const BADGE_ACHIEVEMENTS: &str = "SHJIACH_Graduate1\u{2}PAIACH_Login1\u{2}PAJACH_Login2\u{2}PAKACH_Login3\u{2}PAPAACH_Login4\u{2}PAQAACH_Login5\u{2}PBIACH_RoomEntry1\u{2}PBJACH_RoomEntry2\u{2}PBKACH_RoomEntry3\u{2}SBRAACH_RegistrationDuration6\u{2}SBSAACH_RegistrationDuration7\u{2}SBPBACH_RegistrationDuration8\u{2}SBQBACH_RegistrationDuration9\u{2}SBRBACH_RegistrationDuration10\u{2}RAIACH_AvatarLooks1\u{2}IJGLB\u{2}IKGLC\u{2}IPAGLD\u{2}IQAGLE\u{2}IRAGLF\u{2}ISAGLG\u{2}IPBGLH\u{2}IQBGLI\u{2}IRBGLJ\u{2}SAIACH_Student1\u{2}PCIHC1\u{2}PCJHC2\u{2}PCKHC3\u{2}PCPAHC4\u{2}PCQAHC5\u{2}QAIACH_GamePlayed1\u{2}QAJACH_GamePlayed2\u{2}QAKACH_GamePlayed3\u{2}QAPAACH_GamePlayed4\u{2}QAQAACH_GamePlayed5\u{2}";
const SPRITE_INDEX: &str = "[SEshelves_norja\u{2}X~Dshelves_polyfon\u{2}YmAshelves_silo\u{2}XQHtable_polyfon_small\u{2}YmAchair_polyfon\u{2}ZbBtable_norja_med\u{2}Y_Itable_silo_med\u{2}X~Dtable_plasto_4leg\u{2}Y_Itable_plasto_round\u{2}Y_Itable_plasto_bigsquare\u{2}Y_Istand_polyfon_z\u{2}ZbBchair_silo\u{2}X~Dsofa_silo\u{2}X~Dcouch_norja\u{2}X~Dchair_norja\u{2}X~Dtable_polyfon_med\u{2}YmAdoormat_love\u{2}ZbBdoormat_plain\u{2}Z[Msofachair_polyfon\u{2}X~Dsofa_polyfon\u{2}Z[Msofachair_silo\u{2}X~Dchair_plasty\u{2}X~Dchair_plasto\u{2}YmAtable_plasto_square\u{2}Y_Ibed_polyfon\u{2}X~Dbed_polyfon_one\u{2}[dObed_trad_one\u{2}YmAbed_trad\u{2}YmAbed_silo_one\u{2}YmAbed_silo_two\u{2}YmAtable_silo_small\u{2}X~Dbed_armas_two\u{2}YmAbed_budget_one\u{2}XQHbed_budget\u{2}XQHshelves_armas\u{2}YmAbench_armas\u{2}YmAtable_armas\u{2}YmAsmall_table_armas\u{2}ZbBsmall_chair_armas\u{2}YmAfireplace_armas\u{2}YmAlamp_armas\u{2}YmAbed_armas_one\u{2}YmAcarpet_standard\u{2}Y_Icarpet_armas\u{2}YmAcarpet_polar\u{2}Y_Ifireplace_polyfon\u{2}Y_Itable_plasto_4leg*1\u{2}Y_Itable_plasto_bigsquare*1\u{2}Y_Itable_plasto_round*1\u{2}Y_Itable_plasto_square*1\u{2}Y_Ichair_plasto*1\u{2}YmAcarpet_standard*1\u{2}Y_Idoormat_plain*1\u{2}Z[Mtable_plasto_4leg*2\u{2}Y_Itable_plasto_bigsquare*2\u{2}Y_Itable_plasto_round*2\u{2}Y_Itable_plasto_square*2\u{2}Y_Ichair_plasto*2\u{2}YmAdoormat_plain*2\u{2}Z[Mcarpet_standard*2\u{2}Y_Itable_plasto_4leg*3\u{2}Y_Itable_plasto_bigsquare*3\u{2}Y_Itable_plasto_round*3\u{2}Y_Itable_plasto_square*3\u{2}Y_Ichair_plasto*3\u{2}YmAcarpet_standard*3\u{2}Y_Idoormat_plain*3\u{2}Z[Mtable_plasto_4leg*4\u{2}Y_Itable_plasto_bigsquare*4\u{2}Y_Itable_plasto_round*4\u{2}Y_Itable_plasto_square*4\u{2}Y_Ichair_plasto*4\u{2}YmAcarpet_standard*4\u{2}Y_Idoormat_plain*4\u{2}Z[Mdoormat_plain*6\u{2}Z[Mdoormat_plain*5\u{2}Z[Mcarpet_standard*5\u{2}Y_Itable_plasto_4leg*5\u{2}Y_Itable_plasto_bigsquare*5\u{2}Y_Itable_plasto_round*5\u{2}Y_Itable_plasto_square*5\u{2}Y_Ichair_plasto*5\u{2}YmAtable_plasto_4leg*6\u{2}Y_Itable_plasto_bigsquare*6\u{2}Y_Itable_plasto_round*6\u{2}Y_Itable_plasto_square*6\u{2}Y_Ichair_plasto*6\u{2}YmAtable_plasto_4leg*7\u{2}Y_Itable_plasto_bigsquare*7\u{2}Y_Itable_plasto_round*7\u{2}Y_Itable_plasto_square*7\u{2}Y_Ichair_plasto*7\u{2}YmAtable_plasto_4leg*8\u{2}Y_Itable_plasto_bigsquare*8\u{2}Y_Itable_plasto_round*8\u{2}Y_Itable_plasto_square*8\u{2}Y_Ichair_plasto*8\u{2}YmAtable_plasto_4leg*9\u{2}Y_Itable_plasto_bigsquare*9\u{2}Y_Itable_plasto_round*9\u{2}Y_Itable_plasto_square*9\u{2}Y_Ichair_plasto*9\u{2}YmAcarpet_standard*6\u{2}Y_Ichair_plasty*1\u{2}X~Dpizza\u{2}YmAdrinks\u{2}YmAchair_plasty*2\u{2}X~Dchair_plasty*3\u{2}X~Dchair_plasty*4\u{2}X~Dbar_polyfon\u{2}Y_Iplant_cruddy\u{2}YmAbottle\u{2}YmAbardesk_polyfon\u{2}X~Dbardeskcorner_polyfon\u{2}X~Dfloortile\u{2}Hbar_armas\u{2}Y_Ibartable_armas\u{2}YmAbar_chair_armas\u{2}YmAcarpet_soft\u{2}Z@Kcarpet_soft*1\u{2}Z@Kcarpet_soft*2\u{2}Z@Kcarpet_soft*3\u{2}Z@Kcarpet_soft*4\u{2}Z@Kcarpet_soft*5\u{2}Z@Kcarpet_soft*6\u{2}Z@Kred_tv\u{2}Y_Iwood_tv\u{2}YmAcarpet_polar*1\u{2}Y_Ichair_plasty*5\u{2}X~Dcarpet_polar*2\u{2}Y_Icarpet_polar*3\u{2}Y_Icarpet_polar*4\u{2}Y_Ichair_plasty*6\u{2}X~Dtable_polyfon\u{2}YmAsmooth_table_polyfon\u{2}YmAsofachair_polyfon_girl\u{2}X~Dbed_polyfon_girl_one\u{2}[dObed_polyfon_girl\u{2}X~Dsofa_polyfon_girl\u{2}Z[Mbed_budgetb_one\u{2}XQHbed_budgetb\u{2}XQHplant_pineapple\u{2}YmAplant_fruittree\u{2}Y_Iplant_small_cactus\u{2}Y_Iplant_bonsai\u{2}Y_Iplant_big_cactus\u{2}Y_Iplant_yukka\u{2}Y_Icarpet_standard*7\u{2}Y_Icarpet_standard*8\u{2}Y_Icarpet_standard*9\u{2}Y_Icarpet_standard*a\u{2}Y_Icarpet_standard*b\u{2}Y_Iplant_sunflower\u{2}Y_Iplant_rose\u{2}Y_Itv_luxus\u{2}Y_Ibath\u{2}Z\\Bsink\u{2}Y_Itoilet\u{2}YmAduck\u{2}YmAtile\u{2}YmAtoilet_red\u{2}YmAtoilet_yell\u{2}YmAtile_red\u{2}YmAtile_yell\u{2}YmApresent_gen\u{2}[~Npresent_gen1\u{2}[~Npresent_gen2\u{2}[~Npresent_gen3\u{2}[~Npresent_gen4\u{2}[~Npresent_gen5\u{2}[~Npresent_gen6\u{2}[~Nbar_basic\u{2}Y_Ishelves_basic\u{2}XQHsoft_sofachair_norja\u{2}X~Dsoft_sofa_norja\u{2}X~Dlamp_basic\u{2}XQHlamp2_armas\u{2}YmAfridge\u{2}Y_Idoor\u{2}Yc[doorB\u{2}Yc[doorC\u{2}Yc[pumpkin\u{2}YmAskullcandle\u{2}YmAdeadduck\u{2}YmAdeadduck2\u{2}YmAdeadduck3\u{2}YmAmenorah\u{2}YmApudding\u{2}YmAham\u{2}YmAturkey\u{2}YmAxmasduck\u{2}Y_Ihouse\u{2}YmAtriplecandle\u{2}YmAtree3\u{2}YmAtree4\u{2}YmAtree5\u{2}X~Dham2\u{2}YmAwcandleset\u{2}YmArcandleset\u{2}YmAstatue\u{2}YmAheart\u{2}Y_Ivaleduck\u{2}YmAheartsofa\u{2}X~Dthrone\u{2}YmAsamovar\u{2}Y_Igiftflowers\u{2}Y_Ihabbocake\u{2}YmAhologram\u{2}YmAeasterduck\u{2}Y_Ibunny\u{2}YmAbasket\u{2}Y_Ibirdie\u{2}YmAedice\u{2}X~Dclub_sofa\u{2}Z[Mprize1\u{2}YmAprize2\u{2}YmAprize3\u{2}YmAdivider_poly3\u{2}X~Ddivider_arm1\u{2}YmAdivider_arm2\u{2}YmAdivider_arm3\u{2}YmAdivider_nor1\u{2}X~Ddivider_silo1\u{2}X~Ddivider_nor2\u{2}X~Ddivider_silo2\u{2}Z[Mdivider_nor3\u{2}X~Ddivider_silo3\u{2}X~Dtypingmachine\u{2}YmAspyro\u{2}YmAredhologram\u{2}YmAcamera\u{2}Hjoulutahti\u{2}YmAhyacinth1\u{2}YmAhyacinth2\u{2}YmAchair_plasto*10\u{2}YmAchair_plasto*11\u{2}YmAbardeskcorner_polyfon*12\u{2}X~Dbardeskcorner_polyfon*13\u{2}X~Dchair_plasto*12\u{2}YmAchair_plasto*13\u{2}YmAchair_plasto*14\u{2}YmAtable_plasto_4leg*14\u{2}Y_Imocchamaster\u{2}Y_Icarpet_legocourt\u{2}YmAbench_lego\u{2}YmAlegotrophy\u{2}YmAvalentinescreen\u{2}YmAedicehc\u{2}YmArare_daffodil_rug\u{2}YmArare_beehive_bulb\u{2}Y_Ihcsohva\u{2}YmAhcamme\u{2}YmArare_elephant_statue\u{2}YmArare_fountain\u{2}Y_Irare_stand\u{2}YmArare_globe\u{2}YmArare_hammock\u{2}YmArare_elephant_statue*1\u{2}YmArare_elephant_statue*2\u{2}YmArare_fountain*1\u{2}Y_Irare_fountain*2\u{2}Y_Irare_fountain*3\u{2}Y_Irare_beehive_bulb*1\u{2}Y_Irare_beehive_bulb*2\u{2}Y_Irare_xmas_screen\u{2}Y_Irare_parasol*1\u{2}XMVrare_parasol*2\u{2}XMVrare_parasol*3\u{2}XMVtree1\u{2}X~Dtree2\u{2}ZmBwcandle\u{2}YxBrcandle\u{2}YxBsoft_jaggara_norja\u{2}YmAhouse2\u{2}YmAdjesko_turntable\u{2}YmAmd_sofa\u{2}Z[Mmd_limukaappi\u{2}Y_Itable_plasto_4leg*10\u{2}Y_Itable_plasto_4leg*15\u{2}Y_Itable_plasto_bigsquare*14\u{2}Y_Itable_plasto_bigsquare*15\u{2}Y_Itable_plasto_round*14\u{2}Y_Itable_plasto_round*15\u{2}Y_Itable_plasto_square*14\u{2}Y_Itable_plasto_square*15\u{2}Y_Ichair_plasto*15\u{2}YmAchair_plasty*7\u{2}X~Dchair_plasty*8\u{2}X~Dchair_plasty*9\u{2}X~Dchair_plasty*10\u{2}X~Dchair_plasty*11\u{2}X~Dchair_plasto*16\u{2}YmAtable_plasto_4leg*16\u{2}Y_Ihockey_score\u{2}Y_Ihockey_light\u{2}YmAdoorD\u{2}Yc[prizetrophy2*3\u{2}Yd[prizetrophy3*3\u{2}Yd[prizetrophy4*3\u{2}Yd[prizetrophy5*3\u{2}Yd[prizetrophy6*3\u{2}Yd[prizetrophy*1\u{2}Yd[prizetrophy2*1\u{2}Yd[prizetrophy3*1\u{2}Yd[prizetrophy4*1\u{2}Yd[prizetrophy5*1\u{2}Yd[prizetrophy6*1\u{2}Yd[prizetrophy*2\u{2}Yd[prizetrophy2*2\u{2}Yd[prizetrophy3*2\u{2}Yd[prizetrophy4*2\u{2}Yd[prizetrophy5*2\u{2}Yd[prizetrophy6*2\u{2}Yd[prizetrophy*3\u{2}Yd[rare_parasol*0\u{2}XMVhc_lmp\u{2}[fBhc_tbl\u{2}YmAhc_chr\u{2}YmAhc_dsk\u{2}XQHnest\u{2}Hpetfood1\u{2}ZvCpetfood2\u{2}ZvCpetfood3\u{2}ZvCwaterbowl*4\u{2}XICwaterbowl*5\u{2}XICwaterbowl*2\u{2}XICwaterbowl*1\u{2}XICwaterbowl*3\u{2}XICtoy1\u{2}XICtoy1*1\u{2}XICtoy1*2\u{2}XICtoy1*3\u{2}XICtoy1*4\u{2}XICgoodie1\u{2}Yc[goodie1*1\u{2}Yc[goodie1*2\u{2}Yc[goodie2\u{2}Yc[prizetrophy7*3\u{2}Yd[prizetrophy7*1\u{2}Yd[prizetrophy7*2\u{2}Yd[scifiport*0\u{2}Y_Iscifiport*9\u{2}Y_Iscifiport*8\u{2}Y_Iscifiport*7\u{2}Y_Iscifiport*6\u{2}Y_Iscifiport*5\u{2}Y_Iscifiport*4\u{2}Y_Iscifiport*3\u{2}Y_Iscifiport*2\u{2}Y_Iscifiport*1\u{2}Y_Iscifirocket*9\u{2}Y_Iscifirocket*8\u{2}Y_Iscifirocket*7\u{2}Y_Iscifirocket*6\u{2}Y_Iscifirocket*5\u{2}Y_Iscifirocket*4\u{2}Y_Iscifirocket*3\u{2}Y_Iscifirocket*2\u{2}Y_Iscifirocket*1\u{2}Y_Iscifirocket*0\u{2}Y_Iscifidoor*10\u{2}Y_Iscifidoor*9\u{2}Y_Iscifidoor*8\u{2}Y_Iscifidoor*7\u{2}Y_Iscifidoor*6\u{2}Y_Iscifidoor*5\u{2}Y_Iscifidoor*4\u{2}Y_Iscifidoor*3\u{2}Y_Iscifidoor*2\u{2}Y_Iscifidoor*1\u{2}Y_Ipillow*5\u{2}YmApillow*8\u{2}YmApillow*0\u{2}YmApillow*1\u{2}YmApillow*2\u{2}YmApillow*7\u{2}YmApillow*9\u{2}YmApillow*4\u{2}YmApillow*6\u{2}YmApillow*3\u{2}YmAmarquee*1\u{2}Y_Imarquee*2\u{2}Y_Imarquee*7\u{2}Y_Imarquee*a\u{2}Y_Imarquee*8\u{2}Y_Imarquee*9\u{2}Y_Imarquee*5\u{2}Y_Imarquee*4\u{2}Y_Imarquee*6\u{2}Y_Imarquee*3\u{2}Y_Iwooden_screen*1\u{2}Y_Iwooden_screen*2\u{2}Y_Iwooden_screen*7\u{2}Y_Iwooden_screen*0\u{2}Y_Iwooden_screen*8\u{2}Y_Iwooden_screen*5\u{2}Y_Iwooden_screen*9\u{2}Y_Iwooden_screen*4\u{2}Y_Iwooden_screen*6\u{2}Y_Iwooden_screen*3\u{2}Y_Ipillar*6\u{2}Y_Ipillar*1\u{2}Y_Ipillar*9\u{2}Y_Ipillar*0\u{2}Y_Ipillar*8\u{2}Y_Ipillar*2\u{2}Y_Ipillar*5\u{2}Y_Ipillar*4\u{2}Y_Ipillar*7\u{2}Y_Ipillar*3\u{2}Y_Irare_dragonlamp*4\u{2}Y_Irare_dragonlamp*0\u{2}Y_Irare_dragonlamp*5\u{2}Y_Irare_dragonlamp*2\u{2}Y_Irare_dragonlamp*8\u{2}Y_Irare_dragonlamp*9\u{2}Y_Irare_dragonlamp*7\u{2}Y_Irare_dragonlamp*6\u{2}Y_Irare_dragonlamp*1\u{2}Y_Irare_dragonlamp*3\u{2}Y_Irare_icecream*1\u{2}Y_Irare_icecream*7\u{2}Y_Irare_icecream*8\u{2}Y_Irare_icecream*2\u{2}Y_Irare_icecream*6\u{2}Y_Irare_icecream*9\u{2}Y_Irare_icecream*3\u{2}Y_Irare_icecream*0\u{2}Y_Irare_icecream*4\u{2}Y_Irare_icecream*5\u{2}Y_Irare_fan*7\u{2}YxBrare_fan*6\u{2}YxBrare_fan*9\u{2}YxBrare_fan*3\u{2}YxBrare_fan*0\u{2}YxBrare_fan*4\u{2}YxBrare_fan*5\u{2}YxBrare_fan*1\u{2}YxBrare_fan*8\u{2}YxBrare_fan*2\u{2}YxBqueue_tile1*3\u{2}X~Dqueue_tile1*6\u{2}X~Dqueue_tile1*4\u{2}X~Dqueue_tile1*9\u{2}X~Dqueue_tile1*8\u{2}X~Dqueue_tile1*5\u{2}X~Dqueue_tile1*7\u{2}X~Dqueue_tile1*2\u{2}X~Dqueue_tile1*1\u{2}X~Dqueue_tile1*0\u{2}X~Dticket\u{2}Hrare_snowrug\u{2}X~Dcn_lamp\u{2}ZxIcn_sofa\u{2}YmAsporttrack1*1\u{2}YmAsporttrack1*3\u{2}YmAsporttrack1*2\u{2}YmAsporttrack2*1\u{2}[~Nsporttrack2*2\u{2}[~Nsporttrack2*3\u{2}[~Nsporttrack3*1\u{2}YmAsporttrack3*2\u{2}YmAsporttrack3*3\u{2}YmAfootylamp\u{2}X~Dbarchair_silo\u{2}X~Ddivider_nor4*4\u{2}X~Dtraffic_light*1\u{2}ZxItraffic_light*2\u{2}ZxItraffic_light*3\u{2}ZxItraffic_light*4\u{2}ZxItraffic_light*6\u{2}ZxIrubberchair*1\u{2}X~Drubberchair*2\u{2}X~Drubberchair*3\u{2}X~Drubberchair*4\u{2}X~Drubberchair*5\u{2}X~Drubberchair*6\u{2}X~Dbarrier*1\u{2}X~Dbarrier*2\u{2}X~Dbarrier*3\u{2}X~Drubberchair*7\u{2}X~Drubberchair*8\u{2}X~Dtable_norja_med*2\u{2}Y_Itable_norja_med*3\u{2}Y_Itable_norja_med*4\u{2}Y_Itable_norja_med*5\u{2}Y_Itable_norja_med*6\u{2}Y_Itable_norja_med*7\u{2}Y_Itable_norja_med*8\u{2}Y_Itable_norja_med*9\u{2}Y_Icouch_norja*2\u{2}X~Dcouch_norja*3\u{2}X~Dcouch_norja*4\u{2}X~Dcouch_norja*5\u{2}X~Dcouch_norja*6\u{2}X~Dcouch_norja*7\u{2}X~Dcouch_norja*8\u{2}X~Dcouch_norja*9\u{2}X~Dshelves_norja*2\u{2}X~Dshelves_norja*3\u{2}X~Dshelves_norja*4\u{2}X~Dshelves_norja*5\u{2}X~Dshelves_norja*6\u{2}X~Dshelves_norja*7\u{2}X~Dshelves_norja*8\u{2}X~Dshelves_norja*9\u{2}X~Dchair_norja*2\u{2}X~Dchair_norja*3\u{2}X~Dchair_norja*4\u{2}X~Dchair_norja*5\u{2}X~Dchair_norja*6\u{2}X~Dchair_norja*7\u{2}X~Dchair_norja*8\u{2}X~Dchair_norja*9\u{2}X~Ddivider_nor1*2\u{2}X~Ddivider_nor1*3\u{2}X~Ddivider_nor1*4\u{2}X~Ddivider_nor1*5\u{2}X~Ddivider_nor1*6\u{2}X~Ddivider_nor1*7\u{2}X~Ddivider_nor1*8\u{2}X~Ddivider_nor1*9\u{2}X~Dsoft_sofa_norja*2\u{2}X~Dsoft_sofa_norja*3\u{2}X~Dsoft_sofa_norja*4\u{2}X~Dsoft_sofa_norja*5\u{2}X~Dsoft_sofa_norja*6\u{2}X~Dsoft_sofa_norja*7\u{2}X~Dsoft_sofa_norja*8\u{2}X~Dsoft_sofa_norja*9\u{2}X~Dsoft_sofachair_norja*2\u{2}X~Dsoft_sofachair_norja*3\u{2}X~Dsoft_sofachair_norja*4\u{2}X~Dsoft_sofachair_norja*5\u{2}X~Dsoft_sofachair_norja*6\u{2}X~Dsoft_sofachair_norja*7\u{2}X~Dsoft_sofachair_norja*8\u{2}X~Dsoft_sofachair_norja*9\u{2}X~Dsofachair_silo*2\u{2}X~Dsofachair_silo*3\u{2}X~Dsofachair_silo*4\u{2}X~Dsofachair_silo*5\u{2}X~Dsofachair_silo*6\u{2}X~Dsofachair_silo*7\u{2}X~Dsofachair_silo*8\u{2}X~Dsofachair_silo*9\u{2}X~Dtable_silo_small*2\u{2}X~Dtable_silo_small*3\u{2}X~Dtable_silo_small*4\u{2}X~Dtable_silo_small*5\u{2}X~Dtable_silo_small*6\u{2}X~Dtable_silo_small*7\u{2}X~Dtable_silo_small*8\u{2}X~Dtable_silo_small*9\u{2}X~Ddivider_silo1*2\u{2}X~Ddivider_silo1*3\u{2}X~Ddivider_silo1*4\u{2}X~Ddivider_silo1*5\u{2}X~Ddivider_silo1*6\u{2}X~Ddivider_silo1*7\u{2}X~Ddivider_silo1*8\u{2}X~Ddivider_silo1*9\u{2}X~Ddivider_silo3*2\u{2}X~Ddivider_silo3*3\u{2}X~Ddivider_silo3*4\u{2}X~Ddivider_silo3*5\u{2}X~Ddivider_silo3*6\u{2}X~Ddivider_silo3*7\u{2}X~Ddivider_silo3*8\u{2}X~Ddivider_silo3*9\u{2}X~Dtable_silo_med*2\u{2}X~Dtable_silo_med*3\u{2}X~Dtable_silo_med*4\u{2}X~Dtable_silo_med*5\u{2}X~Dtable_silo_med*6\u{2}X~Dtable_silo_med*7\u{2}X~Dtable_silo_med*8\u{2}X~Dtable_silo_med*9\u{2}X~Dsofa_silo*2\u{2}X~Dsofa_silo*3\u{2}X~Dsofa_silo*4\u{2}X~Dsofa_silo*5\u{2}X~Dsofa_silo*6\u{2}X~Dsofa_silo*7\u{2}X~Dsofa_silo*8\u{2}X~Dsofa_silo*9\u{2}X~Dsofachair_polyfon*2\u{2}X~Dsofachair_polyfon*3\u{2}X~Dsofachair_polyfon*4\u{2}X~Dsofachair_polyfon*6\u{2}X~Dsofachair_polyfon*7\u{2}X~Dsofachair_polyfon*8\u{2}X~Dsofachair_polyfon*9\u{2}X~Dsofa_polyfon*2\u{2}Z[Msofa_polyfon*3\u{2}Z[Msofa_polyfon*4\u{2}Z[Msofa_polyfon*6\u{2}Z[Msofa_polyfon*7\u{2}Z[Msofa_polyfon*8\u{2}Z[Msofa_polyfon*9\u{2}Z[Mbed_polyfon*2\u{2}X~Dbed_polyfon*3\u{2}X~Dbed_polyfon*4\u{2}X~Dbed_polyfon*6\u{2}X~Dbed_polyfon*7\u{2}X~Dbed_polyfon*8\u{2}X~Dbed_polyfon*9\u{2}X~Dbed_polyfon_one*2\u{2}[dObed_polyfon_one*3\u{2}[dObed_polyfon_one*4\u{2}[dObed_polyfon_one*6\u{2}[dObed_polyfon_one*7\u{2}[dObed_polyfon_one*8\u{2}[dObed_polyfon_one*9\u{2}[dObardesk_polyfon*2\u{2}X~Dbardesk_polyfon*3\u{2}X~Dbardesk_polyfon*4\u{2}X~Dbardesk_polyfon*5\u{2}X~Dbardesk_polyfon*6\u{2}X~Dbardesk_polyfon*7\u{2}X~Dbardesk_polyfon*8\u{2}X~Dbardesk_polyfon*9\u{2}X~Dbardeskcorner_polyfon*2\u{2}X~Dbardeskcorner_polyfon*3\u{2}X~Dbardeskcorner_polyfon*4\u{2}X~Dbardeskcorner_polyfon*5\u{2}X~Dbardeskcorner_polyfon*6\u{2}X~Dbardeskcorner_polyfon*7\u{2}X~Dbardeskcorner_polyfon*8\u{2}X~Dbardeskcorner_polyfon*9\u{2}X~Ddivider_poly3*2\u{2}X~Ddivider_poly3*3\u{2}X~Ddivider_poly3*4\u{2}X~Ddivider_poly3*5\u{2}X~Ddivider_poly3*6\u{2}X~Ddivider_poly3*7\u{2}X~Ddivider_poly3*8\u{2}X~Ddivider_poly3*9\u{2}X~Dchair_silo*2\u{2}X~Dchair_silo*3\u{2}X~Dchair_silo*4\u{2}X~Dchair_silo*5\u{2}X~Dchair_silo*6\u{2}X~Dchair_silo*7\u{2}X~Dchair_silo*8\u{2}X~Dchair_silo*9\u{2}X~Ddivider_nor3*2\u{2}X~Ddivider_nor3*3\u{2}X~Ddivider_nor3*4\u{2}X~Ddivider_nor3*5\u{2}X~Ddivider_nor3*6\u{2}X~Ddivider_nor3*7\u{2}X~Ddivider_nor3*8\u{2}X~Ddivider_nor3*9\u{2}X~Ddivider_nor2*2\u{2}X~Ddivider_nor2*3\u{2}X~Ddivider_nor2*4\u{2}X~Ddivider_nor2*5\u{2}X~Ddivider_nor2*6\u{2}X~Ddivider_nor2*7\u{2}X~Ddivider_nor2*8\u{2}X~Ddivider_nor2*9\u{2}X~Dsilo_studydesk\u{2}X~Dsolarium_norja\u{2}Y_Isolarium_norja*1\u{2}Y_Isolarium_norja*2\u{2}Y_Isolarium_norja*3\u{2}Y_Isolarium_norja*5\u{2}Y_Isolarium_norja*6\u{2}Y_Isolarium_norja*7\u{2}Y_Isolarium_norja*8\u{2}Y_Isolarium_norja*9\u{2}Y_Isandrug\u{2}X~Drare_moonrug\u{2}YmAchair_china\u{2}YmAchina_table\u{2}YmAsleepingbag*1\u{2}YmAsleepingbag*2\u{2}YmAsleepingbag*3\u{2}YmAsleepingbag*4\u{2}YmAsafe_silo\u{2}Y_Isleepingbag*7\u{2}YmAsleepingbag*9\u{2}YmAsleepingbag*5\u{2}YmAsleepingbag*10\u{2}YmAsleepingbag*6\u{2}YmAsleepingbag*8\u{2}YmAchina_shelve\u{2}X~Dtraffic_light*5\u{2}ZxIdivider_nor4*2\u{2}X~Ddivider_nor4*3\u{2}X~Ddivider_nor4*5\u{2}X~Ddivider_nor4*6\u{2}X~Ddivider_nor4*7\u{2}X~Ddivider_nor4*8\u{2}X~Ddivider_nor4*9\u{2}X~Ddivider_nor5*2\u{2}X~Ddivider_nor5*3\u{2}X~Ddivider_nor5*4\u{2}X~Ddivider_nor5*5\u{2}X~Ddivider_nor5*6\u{2}X~Ddivider_nor5*7\u{2}X~Ddivider_nor5*8\u{2}X~Ddivider_nor5*9\u{2}X~Ddivider_nor5\u{2}X~Ddivider_nor4\u{2}X~Dwall_china\u{2}YmAcorner_china\u{2}YmAbarchair_silo*2\u{2}X~Dbarchair_silo*3\u{2}X~Dbarchair_silo*4\u{2}X~Dbarchair_silo*5\u{2}X~Dbarchair_silo*6\u{2}X~Dbarchair_silo*7\u{2}X~Dbarchair_silo*8\u{2}X~Dbarchair_silo*9\u{2}X~Dsafe_silo*2\u{2}Y_Isafe_silo*3\u{2}Y_Isafe_silo*4\u{2}Y_Isafe_silo*5\u{2}Y_Isafe_silo*6\u{2}Y_Isafe_silo*7\u{2}Y_Isafe_silo*8\u{2}Y_Isafe_silo*9\u{2}Y_Iglass_shelf\u{2}Y_Iglass_chair\u{2}Y_Iglass_stool\u{2}Y_Iglass_sofa\u{2}Y_Iglass_table\u{2}Y_Iglass_table*2\u{2}Y_Iglass_table*3\u{2}Y_Iglass_table*4\u{2}Y_Iglass_table*5\u{2}Y_Iglass_table*6\u{2}Y_Iglass_table*7\u{2}Y_Iglass_table*8\u{2}Y_Iglass_table*9\u{2}Y_Iglass_chair*2\u{2}Y_Iglass_chair*3\u{2}Y_Iglass_chair*4\u{2}Y_Iglass_chair*5\u{2}Y_Iglass_chair*6\u{2}Y_Iglass_chair*7\u{2}Y_Iglass_chair*8\u{2}Y_Iglass_chair*9\u{2}Y_Iglass_sofa*2\u{2}Y_Iglass_sofa*3\u{2}Y_Iglass_sofa*4\u{2}Y_Iglass_sofa*5\u{2}Y_Iglass_sofa*6\u{2}Y_Iglass_sofa*7\u{2}Y_Iglass_sofa*8\u{2}Y_Iglass_sofa*9\u{2}Y_Iglass_stool*2\u{2}Y_Iglass_stool*4\u{2}Y_Iglass_stool*5\u{2}Y_Iglass_stool*6\u{2}Y_Iglass_stool*7\u{2}Y_Iglass_stool*8\u{2}Y_Iglass_stool*3\u{2}Y_Iglass_stool*9\u{2}Y_ICF_10_coin_gold\u{2}ZvCCF_1_coin_bronze\u{2}ZvCCF_20_moneybag\u{2}ZvCCF_50_goldbar\u{2}ZvCCF_5_coin_silver\u{2}ZvChc_crpt\u{2}YmAhc_tv\u{2}Z\\Bgothgate\u{2}X~Dgothiccandelabra\u{2}YxBgothrailing\u{2}X~Dgoth_table\u{2}YmAhc_bkshlf\u{2}YmAhc_btlr\u{2}Y_Ihc_crtn\u{2}YmAhc_djset\u{2}YmAhc_frplc\u{2}ZbBhc_lmpst\u{2}YmAhc_machine\u{2}YmAhc_rllr\u{2}XQHhc_rntgn\u{2}X~Dhc_trll\u{2}YmAgothic_chair*1\u{2}X~Dgothic_sofa*1\u{2}X~Dgothic_stool*1\u{2}X~Dgothic_chair*2\u{2}X~Dgothic_sofa*2\u{2}X~Dgothic_stool*2\u{2}X~Dgothic_chair*3\u{2}X~Dgothic_sofa*3\u{2}X~Dgothic_stool*3\u{2}X~Dgothic_chair*4\u{2}X~Dgothic_sofa*4\u{2}X~Dgothic_stool*4\u{2}X~Dgothic_chair*5\u{2}X~Dgothic_sofa*5\u{2}X~Dgothic_stool*5\u{2}X~Dgothic_chair*6\u{2}X~Dgothic_sofa*6\u{2}X~Dgothic_stool*6\u{2}X~Dval_cauldron\u{2}X~Dsound_machine\u{2}X~Dromantique_pianochair*3\u{2}Y_Iromantique_pianochair*5\u{2}Y_Iromantique_pianochair*2\u{2}Y_Iromantique_pianochair*4\u{2}Y_Iromantique_pianochair*1\u{2}Y_Iromantique_divan*3\u{2}Y_Iromantique_divan*5\u{2}Y_Iromantique_divan*2\u{2}Y_Iromantique_divan*4\u{2}Y_Iromantique_divan*1\u{2}Y_Iromantique_chair*3\u{2}Y_Iromantique_chair*5\u{2}Y_Iromantique_chair*2\u{2}Y_Iromantique_chair*4\u{2}Y_Iromantique_chair*1\u{2}Y_Irare_parasol\u{2}Y_Iplant_valentinerose*3\u{2}XICplant_valentinerose*5\u{2}XICplant_valentinerose*2\u{2}XICplant_valentinerose*4\u{2}XICplant_valentinerose*1\u{2}XICplant_mazegate\u{2}YeCplant_maze\u{2}ZcCplant_bulrush\u{2}XICpetfood4\u{2}Y_Icarpet_valentine\u{2}Z|Egothic_carpet\u{2}XICgothic_carpet2\u{2}Z|Egothic_chair\u{2}X~Dgothic_sofa\u{2}X~Dgothic_stool\u{2}X~Dgrand_piano*3\u{2}Z|Egrand_piano*5\u{2}Z|Egrand_piano*2\u{2}Z|Egrand_piano*4\u{2}Z|Egrand_piano*1\u{2}Z|Etheatre_seat\u{2}Z@Kromantique_tray2\u{2}Y_Iromantique_tray1\u{2}Y_Iromantique_smalltabl*3\u{2}Y_Iromantique_smalltabl*5\u{2}Y_Iromantique_smalltabl*2\u{2}Y_Iromantique_smalltabl*4\u{2}Y_Iromantique_smalltabl*1\u{2}Y_Iromantique_mirrortabl\u{2}Y_Iromantique_divider*3\u{2}Z[Mromantique_divider*2\u{2}Z[Mromantique_divider*4\u{2}Z[Mromantique_divider*1\u{2}Z[Mjp_tatami2\u{2}[dWjp_tatami\u{2}YGGhabbowood_chair\u{2}YGGjp_bamboo\u{2}YGGjp_irori\u{2}XQHjp_pillow\u{2}YGGsound_set_1\u{2}[dWsound_set_2\u{2}[dWsound_set_3\u{2}[dWsound_set_4\u{2}[dWsound_set_5\u{2}[dWsound_set_6\u{2}[dWsound_set_7\u{2}[dWsound_set_8\u{2}[dWsound_set_9\u{2}[dWsound_machine*1\u{2}Yc[spotlight\u{2}Y_Isound_machine*2\u{2}Yc[sound_machine*3\u{2}Yc[sound_machine*4\u{2}Yc[sound_machine*5\u{2}Yc[sound_machine*6\u{2}Yc[sound_machine*7\u{2}Yc[rom_lamp\u{2}Z|Erclr_sofa\u{2}XQHrclr_garden\u{2}XQHrclr_chair\u{2}Z|Esound_set_28\u{2}[dWsound_set_27\u{2}[dWsound_set_26\u{2}[dWsound_set_25\u{2}[dWsound_set_24\u{2}[dWsound_set_23\u{2}[dWsound_set_22\u{2}[dWsound_set_21\u{2}[dWsound_set_20\u{2}[dWsound_set_19\u{2}[dWsound_set_18\u{2}[dWsound_set_17\u{2}[dWsound_set_16\u{2}[dWsound_set_15\u{2}[dWsound_set_14\u{2}[dWsound_set_13\u{2}[dWsound_set_12\u{2}[dWsound_set_11\u{2}[dWsound_set_10\u{2}[dWrope_divider\u{2}XQHromantique_clock\u{2}Y_Irare_icecream_campaign\u{2}Y_Ipura_mdl5*1\u{2}Yc[pura_mdl5*2\u{2}Yc[pura_mdl5*3\u{2}Yc[pura_mdl5*4\u{2}Yc[pura_mdl5*5\u{2}Yc[pura_mdl5*6\u{2}Yc[pura_mdl5*7\u{2}Yc[pura_mdl5*8\u{2}Yc[pura_mdl5*9\u{2}Yc[pura_mdl4*1\u{2}XQHpura_mdl4*2\u{2}XQHpura_mdl4*3\u{2}XQHpura_mdl4*4\u{2}XQHpura_mdl4*5\u{2}XQHpura_mdl4*6\u{2}XQHpura_mdl4*7\u{2}XQHpura_mdl4*8\u{2}XQHpura_mdl4*9\u{2}XQHpura_mdl3*1\u{2}XQHpura_mdl3*2\u{2}XQHpura_mdl3*3\u{2}XQHpura_mdl3*4\u{2}XQHpura_mdl3*5\u{2}XQHpura_mdl3*6\u{2}XQHpura_mdl3*7\u{2}XQHpura_mdl3*8\u{2}XQHpura_mdl3*9\u{2}XQHpura_mdl2*1\u{2}XQHpura_mdl2*2\u{2}XQHpura_mdl2*3\u{2}XQHpura_mdl2*4\u{2}XQHpura_mdl2*5\u{2}XQHpura_mdl2*6\u{2}XQHpura_mdl2*7\u{2}XQHpura_mdl2*8\u{2}XQHpura_mdl2*9\u{2}XQHpura_mdl1*1\u{2}XQHpura_mdl1*2\u{2}XQHpura_mdl1*3\u{2}XQHpura_mdl1*4\u{2}XQHpura_mdl1*5\u{2}XQHpura_mdl1*6\u{2}XQHpura_mdl1*7\u{2}XQHpura_mdl1*8\u{2}XQHpura_mdl1*9\u{2}XQHjp_lantern\u{2}XQHchair_basic*1\u{2}XQHchair_basic*2\u{2}XQHchair_basic*3\u{2}XQHchair_basic*4\u{2}XQHchair_basic*5\u{2}XQHchair_basic*6\u{2}XQHchair_basic*7\u{2}XQHchair_basic*8\u{2}XQHchair_basic*9\u{2}XQHbed_budget*1\u{2}XQHbed_budget*2\u{2}XQHbed_budget*3\u{2}XQHbed_budget*4\u{2}XQHbed_budget*5\u{2}XQHbed_budget*6\u{2}XQHbed_budget*7\u{2}XQHbed_budget*8\u{2}XQHbed_budget*9\u{2}XQHbed_budget_one*1\u{2}XQHbed_budget_one*2\u{2}XQHbed_budget_one*3\u{2}XQHbed_budget_one*4\u{2}XQHbed_budget_one*5\u{2}XQHbed_budget_one*6\u{2}XQHbed_budget_one*7\u{2}XQHbed_budget_one*8\u{2}XQHbed_budget_one*9\u{2}XQHjp_drawer\u{2}XQHtile_stella\u{2}Z[Mtile_marble\u{2}Z[Mtile_brown\u{2}Z[Msummer_grill*1\u{2}Y_Isummer_grill*2\u{2}Y_Isummer_grill*3\u{2}Y_Isummer_grill*4\u{2}Y_Isummer_chair*1\u{2}Y_Isummer_chair*2\u{2}Y_Isummer_chair*3\u{2}Y_Isummer_chair*4\u{2}Y_Isummer_chair*5\u{2}Y_Isummer_chair*6\u{2}Y_Isummer_chair*7\u{2}Y_Isummer_chair*8\u{2}Y_Isummer_chair*9\u{2}Y_Isound_set_36\u{2}[dWsound_set_35\u{2}[dWsound_set_34\u{2}[dWsound_set_33\u{2}[dWsound_set_32\u{2}[dWsound_set_31\u{2}[dWsound_set_30\u{2}[dWsound_set_29\u{2}[dWsound_machine_pro\u{2}Yc[rare_mnstr\u{2}Y_Ione_way_door*1\u{2}XQHone_way_door*2\u{2}XQHone_way_door*3\u{2}XQHone_way_door*4\u{2}XQHone_way_door*5\u{2}XQHone_way_door*6\u{2}XQHone_way_door*7\u{2}XQHone_way_door*8\u{2}XQHone_way_door*9\u{2}XQHexe_rug\u{2}Z[Mexe_s_table\u{2}ZGRsound_set_37\u{2}[dWsummer_pool*1\u{2}ZlIsummer_pool*2\u{2}ZlIsummer_pool*3\u{2}ZlIsummer_pool*4\u{2}ZlIsong_disk\u{2}Yc[jukebox*1\u{2}Yc[carpet_soft_tut\u{2}[~Nsound_set_44\u{2}[dWsound_set_43\u{2}[dWsound_set_42\u{2}[dWsound_set_41\u{2}[dWsound_set_40\u{2}[dWsound_set_39\u{2}[dWsound_set_38\u{2}[dWgrunge_chair\u{2}Z@Kgrunge_mattress\u{2}Z@Kgrunge_radiator\u{2}Z@Kgrunge_shelf\u{2}Z@Kgrunge_sign\u{2}Z@Kgrunge_table\u{2}Z@Khabboween_crypt\u{2}[uKhabboween_grass\u{2}Z@Khal_cauldron\u{2}Z@Khal_grave\u{2}Z@Ksound_set_52\u{2}[dWsound_set_51\u{2}[dWsound_set_50\u{2}[dWsound_set_49\u{2}[dWsound_set_48\u{2}[dWsound_set_47\u{2}[dWsound_set_46\u{2}[dWsound_set_45\u{2}[dWxmas_icelamp\u{2}Z[Mxmas_cstl_wall\u{2}Z[Mxmas_cstl_twr\u{2}Z[Mxmas_cstl_gate\u{2}[~Ntree7\u{2}Z[Mtree6\u{2}Z[Msound_set_54\u{2}[dWsound_set_53\u{2}[dWsafe_silo_pb\u{2}[dOplant_mazegate_snow\u{2}Z[Mplant_maze_snow\u{2}Z[Mchristmas_sleigh\u{2}Z[Mchristmas_reindeer\u{2}[~Nchristmas_poop\u{2}Z[Mexe_bardesk\u{2}Z[Mexe_chair\u{2}Z[Mexe_chair2\u{2}Z[Mexe_corner\u{2}Z[Mexe_drinks\u{2}Z[Mexe_sofa\u{2}Z[Mexe_table\u{2}Z[Msound_set_59\u{2}[dWsound_set_58\u{2}[dWsound_set_57\u{2}[dWsound_set_56\u{2}[dWsound_set_55\u{2}[dWnoob_table*1\u{2}[~Nnoob_table*2\u{2}[~Nnoob_table*3\u{2}[~Nnoob_table*4\u{2}[~Nnoob_table*5\u{2}[~Nnoob_table*6\u{2}[~Nnoob_stool*1\u{2}[~Nnoob_stool*2\u{2}[~Nnoob_stool*3\u{2}[~Nnoob_stool*4\u{2}[~Nnoob_stool*5\u{2}[~Nnoob_stool*6\u{2}[~Nnoob_rug*1\u{2}[~Nnoob_rug*2\u{2}[~Nnoob_rug*3\u{2}[~Nnoob_rug*4\u{2}[~Nnoob_rug*5\u{2}[~Nnoob_rug*6\u{2}[~Nnoob_lamp*1\u{2}[dOnoob_lamp*2\u{2}[dOnoob_lamp*3\u{2}[dOnoob_lamp*4\u{2}[dOnoob_lamp*5\u{2}[dOnoob_lamp*6\u{2}[dOnoob_chair*1\u{2}[~Nnoob_chair*2\u{2}[~Nnoob_chair*3\u{2}[~Nnoob_chair*4\u{2}[~Nnoob_chair*5\u{2}[~Nnoob_chair*6\u{2}[~Nexe_globe\u{2}[~Nexe_plant\u{2}Z[Mval_teddy*1\u{2}[dOval_teddy*2\u{2}[dOval_teddy*3\u{2}[dOval_teddy*4\u{2}[dOval_teddy*5\u{2}[dOval_teddy*6\u{2}[dOval_randomizer\u{2}[dOval_choco\u{2}[dOteleport_door\u{2}Yc[sound_set_61\u{2}[dWsound_set_60\u{2}[dWfortune\u{2}[dOsw_table\u{2}ZIPsw_raven\u{2}[cQsw_chest\u{2}ZIPsand_cstl_wall\u{2}ZIPsand_cstl_twr\u{2}ZIPsand_cstl_gate\u{2}ZIPgrunge_candle\u{2}ZIPgrunge_bench\u{2}ZIPgrunge_barrel\u{2}ZIPrclr_lamp\u{2}ZGRprizetrophy9*1\u{2}Yd[prizetrophy8*1\u{2}Yd[nouvelle_trax\u{2}Yc[md_rug\u{2}ZGRjp_tray6\u{2}ZGRjp_tray5\u{2}ZGRjp_tray4\u{2}ZGRjp_tray3\u{2}ZGRjp_tray2\u{2}ZGRjp_tray1\u{2}ZGRarabian_teamk\u{2}ZGRarabian_snake\u{2}ZGRarabian_rug\u{2}ZGRarabian_pllw\u{2}ZGRarabian_divdr\u{2}ZGRarabian_chair\u{2}ZGRarabian_bigtb\u{2}ZGRarabian_tetbl\u{2}ZGRarabian_tray1\u{2}ZGRarabian_tray2\u{2}ZGRarabian_tray3\u{2}ZGRarabian_tray4\u{2}ZGRsound_set_64\u{2}[dWsound_set_63\u{2}[dWsound_set_62\u{2}[dWjukebox_ptv*1\u{2}Yc[calippo\u{2}ZAStraxsilver\u{2}Yc[traxgold\u{2}Yc[traxbronze\u{2}Yc[bench_puffet\u{2}YATCFC_500_goldbar\u{2}ZvCCFC_200_moneybag\u{2}ZvCCFC_10_coin_bronze\u{2}ZvCCFC_100_coin_gold\u{2}ZvCCFC_50_coin_silver\u{2}ZvCjp_table\u{2}XMVjp_rare\u{2}XMVjp_katana3\u{2}XMVjp_katana2\u{2}XMVjp_katana1\u{2}XMVfootylamp_campaign\u{2}XMVtiki_waterfall\u{2}[dWtiki_tray4\u{2}[dWtiki_tray3\u{2}[dWtiki_tray2\u{2}[dWtiki_tray1\u{2}[dWtiki_tray0\u{2}[dWtiki_toucan\u{2}[dWtiki_torch\u{2}[dWtiki_statue\u{2}[dWtiki_sand\u{2}[dWtiki_parasol\u{2}[dWtiki_junglerug\u{2}[dWtiki_corner\u{2}[dWtiki_bflies\u{2}[dWtiki_bench\u{2}[dWtiki_bardesk\u{2}[dWtampax_rug\u{2}[dWsound_set_70\u{2}[dWsound_set_69\u{2}[dWsound_set_68\u{2}[dWsound_set_67\u{2}[dWsound_set_66\u{2}[dWsound_set_65\u{2}[dWnoob_rug_tradeable*1\u{2}[dWnoob_rug_tradeable*2\u{2}[dWnoob_rug_tradeable*3\u{2}[dWnoob_rug_tradeable*4\u{2}[dWnoob_rug_tradeable*5\u{2}[dWnoob_rug_tradeable*6\u{2}[dWnoob_plant\u{2}[dWnoob_lamp_tradeable*1\u{2}[dWnoob_lamp_tradeable*2\u{2}[dWnoob_lamp_tradeable*3\u{2}[dWnoob_lamp_tradeable*4\u{2}[dWnoob_lamp_tradeable*5\u{2}[dWnoob_lamp_tradeable*6\u{2}[dWnoob_chair_tradeable*1\u{2}[dWnoob_chair_tradeable*2\u{2}[dWnoob_chair_tradeable*3\u{2}[dWnoob_chair_tradeable*4\u{2}[dWnoob_chair_tradeable*5\u{2}[dWnoob_chair_tradeable*6\u{2}[dWjp_teamaker\u{2}[dWsvnr_uk\u{2}[`_svnr_nl\u{2}XhXsvnr_it\u{2}XhXsvnr_de\u{2}[gXsvnr_aus\u{2}[gXdiner_tray_7\u{2}[gXdiner_tray_6\u{2}[gXdiner_tray_5\u{2}[gXdiner_tray_4\u{2}[gXdiner_tray_3\u{2}[gXdiner_tray_2\u{2}[gXdiner_tray_1\u{2}[gXdiner_tray_0\u{2}[gXdiner_sofa_2*1\u{2}[gXdiner_sofa_2*2\u{2}[gXdiner_sofa_2*3\u{2}[gXdiner_sofa_2*4\u{2}[gXdiner_sofa_2*5\u{2}[gXdiner_sofa_2*6\u{2}[gXdiner_sofa_2*7\u{2}[gXdiner_sofa_2*8\u{2}[gXdiner_sofa_2*9\u{2}[gXdiner_shaker\u{2}[gXdiner_rug\u{2}[gXdiner_gumvendor*1\u{2}[gXdiner_gumvendor*2\u{2}[gXdiner_gumvendor*3\u{2}[gXdiner_gumvendor*4\u{2}[gXdiner_gumvendor*5\u{2}[gXdiner_gumvendor*6\u{2}[gXdiner_gumvendor*7\u{2}[gXdiner_gumvendor*8\u{2}[gXdiner_gumvendor*9\u{2}[gXdiner_cashreg*1\u{2}[gXdiner_cashreg*2\u{2}[gXdiner_cashreg*3\u{2}[gXdiner_cashreg*4\u{2}[gXdiner_cashreg*5\u{2}[gXdiner_cashreg*6\u{2}[gXdiner_cashreg*7\u{2}[gXdiner_cashreg*8\u{2}[gXdiner_cashreg*9\u{2}[gXdiner_table_2*1\u{2}XiZdiner_table_2*2diner_table_2*2\u{2}XiZdiner_table_2*3\u{2}XiZdiner_table_2*4\u{2}XiZdiner_table_2*5\u{2}XiZdiner_table_2*6\u{2}XiZdiner_table_2*7\u{2}XiZdiner_table_2*8\u{2}XiZdiner_table_2*9\u{2}XiZdiner_table_1*1\u{2}XiZdiner_table_1*2\u{2}XiZdiner_table_1*3\u{2}XiZdiner_table_1*4\u{2}XiZdiner_table_1*5\u{2}XiZdiner_table_1*6\u{2}XiZdiner_table_1*7\u{2}XiZdiner_table_1*8\u{2}XiZdiner_table_1*9\u{2}XiZdiner_sofa_1*1\u{2}XiZdiner_sofa_1*2\u{2}XiZdiner_sofa_1*3\u{2}XiZdiner_sofa_1*4\u{2}XiZdiner_sofa_1*5\u{2}XiZdiner_sofa_1*6\u{2}XiZdiner_sofa_1*7\u{2}XiZdiner_sofa_1*8\u{2}XiZdiner_sofa_1*9\u{2}XiZdiner_chair*1\u{2}XiZdiner_chair*2\u{2}XiZdiner_chair*3\u{2}XiZdiner_chair*4\u{2}XiZdiner_chair*5\u{2}XiZdiner_chair*6\u{2}XiZdiner_chair*7\u{2}XiZdiner_chair*8\u{2}XiZdiner_chair*9\u{2}XiZdiner_bardesk_gate*1\u{2}XiZdiner_bardesk_gate*2\u{2}XiZdiner_bardesk_gate*3\u{2}XiZdiner_bardesk_gate*4\u{2}XiZdiner_bardesk_gate*5\u{2}XiZdiner_bardesk_gate*6\u{2}XiZdiner_bardesk_gate*7\u{2}XiZdiner_bardesk_gate*8\u{2}XiZdiner_bardesk_gate*9\u{2}XiZdiner_bardesk_corner*1\u{2}XiZdiner_bardesk_corner*2\u{2}XiZdiner_bardesk_corner*3\u{2}XiZdiner_bardesk_corner*4\u{2}XiZdiner_bardesk_corner*5\u{2}XiZdiner_bardesk_corner*6\u{2}XiZdiner_bardesk_corner*7\u{2}XiZdiner_bardesk_corner*8\u{2}XiZdiner_bardesk_corner*9\u{2}XiZdiner_bardesk*1\u{2}XiZdiner_bardesk*2\u{2}XiZdiner_bardesk*3\u{2}XiZdiner_bardesk*4\u{2}XiZdiner_bardesk*5\u{2}XiZdiner_bardesk*6\u{2}XiZdiner_bardesk*7\u{2}XiZdiner_bardesk*8\u{2}XiZdiner_bardesk*9\u{2}XiZads_dave_cns\u{2}XiZeasy_carpet\u{2}Yc[easy_bowl2\u{2}Yc[greek_corner\u{2}Yc[greek_gate\u{2}Yc[greek_pillars\u{2}Yc[greek_seat\u{2}Yc[greektrophy*1\u{2}[P\\greektrophy*2\u{2}[P\\greektrophy*3\u{2}[P\\greek_block\u{2}Xt[hcc_table\u{2}Y`]hcc_shelf\u{2}Y`]hcc_sofa\u{2}Y`]hcc_minibar\u{2}Y`]hcc_chair\u{2}Y`]det_divider\u{2}Y`]netari_carpet\u{2}Y`]det_body\u{2}Y`]hcc_stool\u{2}Y`]hcc_sofachair\u{2}Y`]hcc_crnr\u{2}Xw]hcc_dvdr\u{2}Xw]sob_carpet\u{2}[`_igor_seat\u{2}[`_ads_igorbrain\u{2}Y_aads_igorswitch\u{2}Y_aads_711*1\u{2}Y_aads_711*2\u{2}Y_aads_711*3\u{2}Y_aads_711*4\u{2}Y_aads_igorraygun\u{2}Y_ahween08_sink\u{2}Y[chween08_curtain\u{2}Y[chween08_bath\u{2}Y[chween08_defibs\u{2}Y[chween08_bbag\u{2}Y[chween08_curtain2\u{2}Y[chween08_defibs2\u{2}Y[chween08_bed\u{2}Y[chween08_sink2\u{2}Y[chween08_bed2\u{2}Y[chween08_bath2\u{2}Y[chween08_manhole\u{2}Y[chween08_trll\u{2}Y[cPRpost.it\u{2}Hpost.it.vd\u{2}Hphoto\u{2}HChess\u{2}HTicTacToe\u{2}HBattleShip\u{2}HPoker\u{2}Hwallpaper\u{2}Hfloor\u{2}Hposter\u{2}Z@Kgothicfountain\u{2}YxBhc_wall_lamp\u{2}ZbBindustrialfan\u{2}Z`Btorch\u{2}Z\\Bval_heart\u{2}XBCwallmirror\u{2}Z|Ejp_ninjastars\u{2}XQHhabw_mirror\u{2}XQHhabbowheel\u{2}Z[Mguitar_skull\u{2}Z@Kguitar_v\u{2}Z@Kxmas_light\u{2}[~Nhrella_poster_3\u{2}[Nhrella_poster_2\u{2}ZIPhrella_poster_1\u{2}[Nsw_swords\u{2}ZIPsw_stone\u{2}ZIPsw_hole\u{2}ZIProomdimmer\u{2}Yc[md_logo_wall\u{2}ZGRmd_can\u{2}ZGRjp_sheet3\u{2}ZGRjp_sheet2\u{2}ZGRjp_sheet1\u{2}ZGRarabian_swords\u{2}ZGRarabian_wndw\u{2}ZGRtiki_wallplnt\u{2}[dWtiki_surfboard\u{2}[dWtampax_wall\u{2}[dWwindow_single_default\u{2}[gXwindow_double_default\u{2}[gXnoob_window_double\u{2}[dWwindow_triple\u{2}[gXwindow_square\u{2}[gXwindow_romantic_wide\u{2}[gXwindow_romantic_narrow\u{2}[gXwindow_grunge\u{2}[gXwindow_golden\u{2}[gXwindow_chinese_wide\u{2}[gXwindow_chinese_narrow\u{2}YA\\window_basic\u{2}[gXwindow_70s_wide\u{2}[gXwindow_70s_narrow\u{2}[gXads_sunnyd\u{2}YlXwindow_diner2\u{2}XiZwindow_diner\u{2}XiZdiner_walltable\u{2}XiZads_dave_wall\u{2}XiZwindow_hole\u{2}Yc[easy_poster\u{2}Yc[ads_nokia_logo\u{2}Yc[ads_nokia_phone\u{2}Yc[landscape\u{2}XV^window_skyscraper\u{2}[j\\netari_poster\u{2}Y`]det_bhole\u{2}Y`]ads_campguitar\u{2}Xw]hween08_rad\u{2}Y[chween08_wndwb\u{2}Y[chween08_wndw\u{2}Y[chween08_bio\u{2}Y[chw_08_xray\u{2}Y[c";

pub async fn run_game_session(
    state: Arc<AppState>,
    connection_id: usize,
    socket: TcpStream,
    remote_ip: String,
) -> Result<()> {
    let ban_reason = user_manager::get_ban_reason_for_ip(&state, &remote_ip).await;
    let (reader, writer) = socket.into_split();
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (disconnect_tx, disconnect_rx) = watch::channel(false);

    let write_task = tokio::spawn(writer_loop(writer, rx));
    if !ban_reason.is_empty() {
        let _ = tx.send(format!("@c{}", ban_reason));
        return Ok(());
    }
    if tx.send("@@".to_string()).is_err() {
        return Ok(());
    }

    let session = Session {
        state,
        connection_id,
        remote_ip,
        tx,
        disconnect_tx,
        logged_in_user_id: None,
        username: None,
        figure: None,
        sex: None,
        mission: None,
        rank: 1,
        welcome_enabled: true,
        hand_page: 0,
        current_room_id: 0,
        current_room_is_public: false,
        room_access_primary_ok: false,
        room_access_secondary_ok: false,
        is_owner: false,
        has_rights: false,
        pending_teleporter_id: 0,
        pending_teleporter_room_id: 0,
        armed_tile_teleport: false,
        current_game_id: None,
        current_game_room_id: None,
        current_game_room_is_public: false,
        current_game_team_id: -1,
        current_room_uid: None,
        song_editor: None,
        hosts_event: false,
        messenger_buddy_presence: HashMap::new(),
        received_sprite_index: false,
        disconnect_rx,
    };

    let read_result = session.read_loop(reader).await;
    write_task.abort();
    read_result
}

struct Session {
    state: Arc<AppState>,
    connection_id: usize,
    remote_ip: String,
    tx: mpsc::UnboundedSender<String>,
    disconnect_tx: watch::Sender<bool>,
    logged_in_user_id: Option<i64>,
    username: Option<String>,
    figure: Option<String>,
    sex: Option<String>,
    mission: Option<String>,
    rank: u8,
    welcome_enabled: bool,
    hand_page: i32,
    current_room_id: i64,
    current_room_is_public: bool,
    room_access_primary_ok: bool,
    room_access_secondary_ok: bool,
    is_owner: bool,
    has_rights: bool,
    pending_teleporter_id: i64,
    pending_teleporter_room_id: i64,
    armed_tile_teleport: bool,
    current_game_id: Option<i64>,
    current_game_room_id: Option<i64>,
    current_game_room_is_public: bool,
    current_game_team_id: i32,
    current_room_uid: Option<i64>,
    song_editor: Option<VirtualSongEditor>,
    hosts_event: bool,
    messenger_buddy_presence: HashMap<i64, (bool, bool)>,
    received_sprite_index: bool,
    disconnect_rx: watch::Receiver<bool>,
}

impl Session {
    async fn read_loop(mut self, mut reader: OwnedReadHalf) -> Result<()> {
        let mut packet_buffer = PacketBuffer::default();
        let mut buffer = [0u8; 4096];

        loop {
            // The original C# emulator could forcibly close a live socket from outside the
            // connection loop. In Rust we mirror that with a watch channel that interrupts reads.
            let bytes_read = tokio::select! {
                result = reader.read(&mut buffer) => result?,
                changed = self.disconnect_rx.changed() => {
                    match changed {
                        Ok(()) if *self.disconnect_rx.borrow() => break,
                        Ok(()) => continue,
                        Err(_) => break,
                    }
                }
            };
            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            if chunk.iter().any(|byte| matches!(byte, 0x05 | 0x09)) {
                warn!(
                    connection_id = self.connection_id,
                    "disconnecting invalid control-byte packet"
                );
                break;
            }

            packet_buffer.push(chunk);
            for packet in packet_buffer.next_packets()? {
                debug!(connection_id = self.connection_id, packet = %packet, "received packet");
                if !self.handle_packet(&packet).await? {
                    self.cleanup().await;
                    return Ok(());
                }
            }
        }

        self.cleanup().await;
        Ok(())
    }

    async fn handle_packet(&mut self, packet: &str) -> Result<bool> {
        if packet.len() < 2 {
            return Ok(false);
        }

        let header = &packet[..2];
        if self.logged_in_user_id.is_none() {
            match header {
                "CD" => return Ok(true),
                "CN" => self.send("DUIH")?,
                "CJ" => self.send("DAQBHHIIKHJIPAHQAdd-MM-yyyy\u{2}SAHPBhotel-co.uk\u{2}QBH")?,
                "_R" => self.send(LOGIN_HANDSHAKE)?,
                "CL" => self.handle_sso_login(packet).await?,
                _ => return Ok(false),
            }
        } else {
            self.reconcile_room_state_with_online_user().await?;
            match header {
                "CD" => {
                    self.mark_ping_ok().await;
                }
                "@q" => self.send(&format!("Bc{}", chrono::Local::now().format("%d/%m/%Y")))?,
                "@L" => {
                    self.send(&format!(
                        "@L{}",
                        virtual_messenger::friend_list(&self.state, self.user_id()?).await?
                    ))?;
                    self.send(&format!(
                        "Dz{}",
                        virtual_messenger::friend_requests(&self.state, self.user_id()?).await?
                    ))?;
                    virtual_messenger::notify_online_friends_of_presence(
                        &self.state,
                        self.user_id()?,
                    )
                    .await?;
                    self.messenger_buddy_presence.clear();
                    let _ = virtual_messenger::build_updates_packet(
                        &self.state,
                        self.user_id()?,
                        &mut self.messenger_buddy_presence,
                    )
                    .await?;
                }
                "@Z" => self.refresh_club().await?,
                "@G" => self.refresh_appearance(false, true, false).await?,
                "@H" => self.refresh_valueables(true, true).await?,
                "@u" => self.leave_room().await?,
                "B]" => self.refresh_badges().await?,
                "Cd" => self.refresh_group_status().await?,
                "C^" => self.send(&format!(
                    "Do{}",
                    recycler_manager::setup_string(&self.state).await
                ))?,
                "C_" => self.send(&format!(
                    "Dp{}",
                    recycler_manager::session_string(&self.state, self.user_id()?).await
                ))?,
                "@B" => self.enter_room_determine(packet).await?,
                "@y" => self.enter_room_check_access(packet).await?,
                "@{" => self.enter_room_guestroom_data().await?,
                "A~" => self.enter_room_publicroom_advertisement().await?,
                "@|" => self.enter_room_load().await?,
                "@}" => self.enter_room_items().await?,
                "@~" => self.enter_room_groups_and_lobby().await?,
                "@\u{7f}" => self.enter_room_wallitems().await?,
                "A@" => self.enter_room_add_user().await?,
                "A`" => self.room_give_rights(packet).await?,
                "AO" => self.room_rotate_user(packet).await?,
                "AK" => self.room_walk_to_square(packet).await?,
                "Aa" => self.room_take_rights(packet).await?,
                "Ab" => self.room_answer_doorbell(packet).await?,
                "AQ" => self.room_enter_teleporter(packet).await?,
                "As" => self.room_click_door().await?,
                "At" => self.room_select_swim_outfit(packet).await?,
                "AZ" => self.room_place_item(packet).await?,
                "AB" => self.room_apply_decor(packet).await?,
                "AC" => self.room_pickup_item(packet).await?,
                "AI" => self.room_move_item(packet).await?,
                "CV" => self.room_toggle_wall_item(packet).await?,
                "AJ" => self.room_toggle_floor_item(packet).await?,
                "AX" => self.status_stop(packet).await?,
                "A^" => self.status_wave().await?,
                "A]" => self.status_dance(packet).await?,
                "Ah" => self.status_lido_vote(packet).await?,
                "AP" => self.status_carry_item(packet).await?,
                "B^" => self.badges_switch(packet).await?,
                "AL" => self.item_spin_dice(packet).await?,
                "AM" => self.item_close_dice(packet).await?,
                "AN" => self.item_open_presentbox(packet).await?,
                "Bw" => self.item_redeem_credit(packet).await?,
                "AG" => self.trade_start(packet).await?,
                "AH" => self.trade_offer_item(packet).await?,
                "AD" => self.trade_decline().await?,
                "AE" => self.trade_accept().await?,
                "AF" => self.trade_abort(true).await?,
                "AS" => self.item_open_sticky(packet).await?,
                "AT" => self.item_edit_sticky(packet).await?,
                "AU" => self.item_delete_sticky(packet).await?,
                "@t" => self.room_chat_say(packet, false).await?,
                "@w" => self.room_chat_say(packet, true).await?,
                "@x" => self.room_chat_whisper(packet).await?,
                "Cl" => self.room_poll_answer(packet).await?,
                "@P" => self.navigator_view_own_rooms().await?,
                "@Q" => self.navigator_room_search(packet).await?,
                "@R" => self.navigator_favourite_rooms().await?,
                "@S" => self.navigator_add_favourite(packet).await?,
                "@T" => self.navigator_remove_favourite(packet).await?,
                "@U" => self.navigator_room_details(packet).await?,
                "@v" => self.enter_room_via_teleporter().await?,
                "@\\" => self.room_use_teleporter(packet).await?,
                "@W" => self.guestroom_delete(packet).await?,
                "@X" => self.guestroom_modify_core(packet).await?,
                "@Y" => self.guestroom_modify_details(packet).await?,
                "@]" => self.guestroom_create_phase_one(packet).await?,
                "@a" => self.messenger_instant_message(packet).await?,
                "@b" => self.messenger_invite_buddies(packet).await?,
                "@e" => self.messenger_accept_requests(packet).await?,
                "@f" => self.messenger_decline_requests(packet).await?,
                "@g" => self.messenger_request_friend(packet).await?,
                "@h" => self.messenger_remove_buddy(packet).await?,
                "@p" => self.call_for_help_pickup(packet).await?,
                "@O" => self.messenger_refresh_updates().await?,
                "D}" => self.room_typing(true).await?,
                "D~" => self.room_typing(false).await?,
                "EA" => self.event_get_setup().await?,
                "E@" => self.room_kick_and_ban(packet).await?,
                "EY" => self.event_host_button_visibility().await?,
                "D{" => self.event_check_category(packet).await?,
                "E^" => self.event_open_category(packet).await?,
                "EZ" => self.event_create(packet).await?,
                "E\\" => self.event_edit(packet).await?,
                "E[" => self.event_end().await?,
                "DE" => self.room_vote(packet).await?,
                "EU" => self.moodlight_load_settings().await?,
                "EV" => self.moodlight_update(packet).await?,
                "EW" => self.moodlight_toggle().await?,
                "B_" => self.game_lobby_refresh().await?,
                "B`" => self.game_lobby_checkout(packet).await?,
                "Bb" => self.game_lobby_request_create().await?,
                "Bc" => self.game_lobby_process_create(packet).await?,
                "Be" => self.game_lobby_switch_team(packet).await?,
                "Bg" => self.game_lobby_leave().await?,
                "Bh" => self.game_lobby_kick(packet).await?,
                "Bj" => self.game_lobby_start().await?,
                "Bk" => self.game_ingame_move(packet).await?,
                "Bl" => self.game_ingame_replay_request().await?,
                "Bv" => self.enter_room_loading_advertisement().await?,
                "BV" => self.navigator_open_category(packet).await?,
                "BW" => self.navigator_room_categories_for_placement().await?,
                "BX" => self.guestroom_modify_trigger(packet).await?,
                "BY" => self.guestroom_modify_category(packet).await?,
                "BZ" => self.navigator_publicroom_userlist(packet).await?,
                "AV" => self.call_for_help_send(packet).await?,
                "CH" => self.modtool_action(packet).await?,
                "HH" => self.modtool_alert_user(packet).await?,
                "HI" => self.modtool_kick_user(packet).await?,
                "HJ" => self.modtool_ban_user(packet).await?,
                "IH" => self.modtool_room_alert(packet).await?,
                "II" => self.modtool_room_kick(packet).await?,
                "CF" => self.call_for_help_delete_by_staff(packet).await?,
                "CG" => self.call_for_help_reply(packet).await?,
                "Cm" => self.call_for_help_status().await?,
                "Cn" => self.call_for_help_delete_own().await?,
                "Cw" => self.room_spin_wheel_of_fortune(packet).await?,
                "DF" => self.messenger_follow_buddy(packet).await?,
                "DH" => self.navigator_recommended_rooms().await?,
                "DG" => self.user_tags(packet).await?,
                "Dz" => self.room_activate_love_shuffler(packet).await?,
                "Cg" => self.group_details(packet).await?,
                "D\u{7f}" => self.ignore_user(packet).await?,
                "EB" => self.unignore_user(packet).await?,
                "EC" => self.call_for_help_go_to_room(packet).await?,
                "Ej" => self.guide_set_available(true).await?,
                "Ek" => self.guide_set_available(false).await?,
                "Fs" => self.console_search_setup().await?,
                "cI" => self.status_ignore_habbo(packet).await?,
                "cK" => self.status_listen_habbo().await?,
                "A_" => self.room_kick_user(packet).await?,
                "Ae" => self.send(&format!(
                    "A~{}",
                    catalogue_manager::get_page_index(&self.state, self.rank).await
                ))?,
                "Af" => {
                    let page_index_name = packet.split('/').nth(1).unwrap_or_default();
                    self.send(&format!(
                        "A\u{7f}{}",
                        catalogue_manager::get_page(&self.state, page_index_name, self.rank).await
                    ))?;
                }
                "Ad" => self.catalogue_purchase(packet).await?,
                "Ca" => self.recycler_proceed(packet).await?,
                "Cb" => self.recycler_redeem_or_cancel(packet).await?,
                "AA" => self.refresh_hand(packet.get(2..).unwrap_or("new")).await?,
                "LB" => self.refresh_hand(packet.get(2..).unwrap_or("new")).await?,
                "Ai" => self.buy_game_tickets(packet).await?,
                "BA" => self.redeem_voucher(packet).await?,
                "Ct" => self.sound_machine_initialize_song_list().await?,
                "Cu" => self.sound_machine_initialize_playlist().await?,
                "C]" => self.sound_machine_get_song(packet).await?,
                "Cs" => self.sound_machine_save_playlist(packet).await?,
                "C~" => self.sound_machine_burn_song_to_disk(packet).await?,
                "Cx" => self.sound_machine_delete_song(packet).await?,
                "Co" => self.sound_machine_editor_initialize().await?,
                "C[" => self.sound_machine_editor_add_soundset(packet).await?,
                "C\\" => self.sound_machine_editor_remove_soundset(packet).await?,
                "Cp" => self.sound_machine_editor_save_new_song(packet).await?,
                "Cq" => self.sound_machine_editor_edit_song(packet).await?,
                "Cr" => self.sound_machine_editor_save_existing_song(packet).await?,
                "@i" => self.console_search(packet).await?,
                _ => {
                    debug!(
                        connection_id = self.connection_id,
                        header, "unported packet"
                    );
                }
            }
        }

        Ok(true)
    }

    async fn handle_sso_login(&mut self, packet: &str) -> Result<()> {
        let ticket = packet
            .get(4..)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("missing SSO ticket in CL packet")?;
        let ticket = Database::stripslash(ticket);

        let user_id = self
            .state
            .db
            .run_read_i64(&format!(
                "SELECT id FROM users WHERE ticket_sso = '{}' LIMIT 1",
                ticket
            ))
            .await?
            .context("no user found for SSO ticket")?;

        let ban_reason = user_manager::get_ban_reason_for_user(&self.state, user_id).await;
        if !ban_reason.is_empty() {
            self.send(&format!("@c{}", ban_reason))?;
            anyhow::bail!("user is banned");
        }

        let user_row = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT name,figure,sex,mission,rank,consolemission,guide FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await?;

        if user_row.len() < 7 {
            anyhow::bail!("incomplete user row for {user_id}");
        }

        let username = user_row[0].clone();
        let figure = user_row[1].clone();
        let sex = user_row[2].clone();
        let mission = user_row[3].clone();
        let rank = user_row[4].parse::<u8>().unwrap_or(1);
        let guide = user_row[6].parse::<i32>().unwrap_or(0);

        let last_ip = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT ipaddress_last FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;

        if !last_ip.is_empty() && !ip_matches_legacy_ticket_ip(&last_ip, &self.remote_ip) {
            warn!(
                connection_id = self.connection_id,
                user_id,
                expected_ip = %last_ip,
                actual_ip = %self.remote_ip,
                "legacy IP ticket check rejected login"
            );
            anyhow::bail!("invalid session ticket for remote IP");
        }

        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET ticket_sso = NULL WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await?;

        self.logged_in_user_id = Some(user_id);
        self.username = Some(username.clone());
        self.figure = Some(figure.clone());
        self.sex = Some(sex.clone());
        self.mission = Some(mission.clone());
        self.rank = rank;
        self.welcome_enabled = string_manager::welcome_message_enabled(&self.state).await?;

        user_manager::add_user(
            &self.state,
            user_id,
            OnlineUser {
                connection_id: self.connection_id,
                user_id,
                username: username.clone(),
                figure: figure.clone(),
                rank,
                in_room: false,
                room_id: 0,
                room_is_public: false,
                hand_page: Arc::new(AtomicI32::new(0)),
                ping_ok: Arc::new(AtomicBool::new(true)),
                is_muted: Arc::new(AtomicBool::new(false)),
                sender: self.tx.clone(),
                disconnect: self.disconnect_tx.clone(),
            },
        )
        .await;

        self.send(&format!(
            "@B{}",
            rank_manager::fuse_rights(&self.state, rank).await?
        ))?;
        self.send("DbIH")?;
        self.send("@C")?;
        if guide == 1 {
            self.send("BKguide")?;
        }
        self.send("FiI")?;
        self.send("FC")?;
        if self.welcome_enabled {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "welcomemessage_text").await?
            ))?;
        }

        let ignored_users = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT targetid FROM user_ignores WHERE userid = '{}' ORDER BY targetid ASC",
                user_id
            ))
            .await
            .unwrap_or_default();
        if !ignored_users.is_empty() {
            let payload = format!(
                "Fd{}{}",
                encode_vl64(ignored_users.len() as i32),
                ignored_users
                    .iter()
                    .map(|id| format!("{id}\u{2}"))
                    .collect::<String>()
            );
            self.send(&payload)?;
        }

        info!(
            connection_id = self.connection_id,
            user_id, username, rank, figure, sex, mission, "user logged in"
        );

        Ok(())
    }

    async fn refresh_appearance(
        &mut self,
        reload: bool,
        refresh_settings: bool,
        refresh_room: bool,
    ) -> Result<()> {
        let user_id = self.user_id()?;
        if reload {
            let user_data = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT figure,sex,mission FROM users WHERE id = '{}' LIMIT 1",
                    user_id
                ))
                .await?;
            if user_data.len() >= 3 {
                self.figure = Some(user_data[0].clone());
                self.sex = Some(user_data[1].clone());
                self.mission = Some(user_data[2].clone());
            }
        }

        if refresh_settings {
            self.send(&format!(
                "@E{}{}{}{}{}{}{}{}H{}HH",
                user_id,
                '\u{2}',
                self.username.clone().unwrap_or_default(),
                '\u{2}',
                self.figure.clone().unwrap_or_default(),
                '\u{2}',
                self.sex.clone().unwrap_or_else(|| "M".to_string()),
                '\u{2}',
                self.mission.clone().unwrap_or_default()
            ))?;
        }

        if refresh_room
            && self.current_room_id > 0
            && self.current_room_uid.is_some()
            && let Some(mut room) = room_manager::get_room(&self.state, self.current_room_id).await
            && let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id)
        {
            // The original C# code mutated the live virtualUser/roomUser object graph directly.
            // In Rust we must patch the shared room snapshot, then broadcast the legacy DJ packet.
            room.users[index].figure = self.figure.clone().unwrap_or_default();
            room.users[index].sex = self.sex.clone().unwrap_or_else(|| "M".to_string());
            room.users[index].mission = self.mission.clone().unwrap_or_default();
            let room_uid = room.users[index].room_uid;
            let refresh_packet = format!(
                "DJ{}{}\u{2}{}\u{2}{}\u{2}",
                encode_vl64(room_uid as i32),
                room.users[index].figure,
                room.users[index].sex,
                room.users[index].mission
            );
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &refresh_packet, None)
                .await;
        }

        if let Some(mut user) = user_manager::get_user(&self.state, user_id).await {
            user.figure = self.figure.clone().unwrap_or_default();
            self.state.online_users.write().await.insert(user_id, user);
        }

        Ok(())
    }

    async fn refresh_valueables(&self, credits: bool, tickets: bool) -> Result<()> {
        let user_id = self.user_id()?;
        if credits {
            let credits = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                    user_id
                ))
                .await;
            self.send(&format!("@F{}", credits))?;
        }

        if tickets {
            let tickets = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                    user_id
                ))
                .await;
            self.send(&format!("A|{}", tickets))?;
        }

        Ok(())
    }

    async fn refresh_club(&self) -> Result<()> {
        let user_id = self.user_id()?;
        let details = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT months_expired,months_left,date_monthstarted FROM users_club WHERE userid = '{}' LIMIT 1",
                user_id
            ))
            .await?;

        let mut resting_days = 0;
        let mut passed_months = 0;
        let mut resting_months = 0;
        if details.len() >= 3 {
            passed_months = details[0].parse::<i32>().unwrap_or(0);
            resting_months = details[1].parse::<i32>().unwrap_or(0) - 1;
            resting_days = parse_club_days(&details[2]);
        }

        self.send(&format!(
            "@Gclub_habbo{}{}{}{}{}",
            '\u{2}',
            encode_vl64(resting_days),
            encode_vl64(passed_months),
            encode_vl64(resting_months),
            encode_vl64(1)
        ))?;
        Ok(())
    }

    async fn is_club_member(&self) -> bool {
        let Ok(user_id) = self.user_id() else {
            return false;
        };

        self.state
            .db
            .check_exists(&format!(
                "SELECT userid FROM users_club WHERE userid = '{}' LIMIT 1",
                user_id
            ))
            .await
    }

    async fn refresh_badges(&self) -> Result<()> {
        let user_id = self.user_id()?;
        let badges = self
            .state
            .db
            .run_read_column_string(&format!(
                "SELECT COALESCE(NULLIF(badge, ''), badgeid) FROM users_badges WHERE userid = '{}' ORDER BY slotid ASC",
                user_id
            ))
            .await?;
        let slot_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT slotid FROM users_badges WHERE userid = '{}' ORDER BY slotid ASC",
                user_id
            ))
            .await?;

        let mut payload = encode_vl64(badges.len() as i32);
        for badge in &badges {
            payload.push_str(badge);
            payload.push('\u{2}');
        }

        for (index, badge) in badges.iter().enumerate() {
            let slot_id = *slot_ids.get(index).unwrap_or(&0);
            if slot_id > 0 {
                payload.push_str(&encode_vl64(slot_id as i32));
                payload.push_str(badge);
                payload.push('\u{2}');
            }
        }

        let current_badges = collect_current_badges(&badges, &slot_ids);
        persist_current_badges(&self.state, user_id, &current_badges).await?;
        self.send(&format!("Ce{}", payload))?;
        self.send(&format!("Ft{}", BADGE_ACHIEVEMENTS))?;
        self.send(&format!("DtIH{}\u{2}FCH", '\u{1}'))?;
        Ok(())
    }

    async fn refresh_group_status(&self) -> Result<()> {
        let user_id = self.user_id()?;
        let (group_id, group_member_rank) = load_current_group_status(&self.state, user_id).await;

        if self.current_room_id > 0
            && let Some(mut room) = room_manager::get_room(&self.state, self.current_room_id).await
            && let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id)
        {
            let previous_group_id = room.users[index].group_id;
            room.users[index].group_id = group_id;
            room.users[index].group_member_rank = group_member_rank;
            if group_id > 0 {
                room.activate_group(group_id);
            }
            if previous_group_id > 0
                && previous_group_id != group_id
                && !room
                    .users
                    .iter()
                    .any(|entry| entry.user_id != user_id && entry.group_id == previous_group_id)
            {
                room.active_group_ids.remove(&previous_group_id);
            }
            room_manager::save_room(&self.state, room).await;
        }

        Ok(())
    }

    async fn badges_switch(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let user_id = self.user_id()?;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users_badges SET slotid = '0' WHERE userid = '{}'",
                user_id
            ))
            .await?;

        let mut enabled_badge_amount = 0_i32;
        let mut remaining = packet.get(2..).unwrap_or_default();
        while !remaining.is_empty() {
            let (slot_id, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => break,
            };
            remaining = &remaining[used..];
            if remaining.len() < 2 {
                break;
            }

            let badge_name_length = decode_b64(&remaining[..2]).unwrap_or(0);
            if remaining.len() < 2 + badge_name_length {
                break;
            }
            if badge_name_length > 0 {
                let badge = &remaining[2..2 + badge_name_length];
                self.state
                    .db
                    .run_query(&format!(
                        "UPDATE users_badges SET slotid = '{}' WHERE userid = '{}' AND (badge = '{}' OR badgeid = '{}') LIMIT 1",
                        slot_id,
                        user_id,
                        Database::stripslash(badge),
                        Database::stripslash(badge)
                    ))
                    .await?;
                enabled_badge_amount += 1;
            }
            remaining = &remaining[2 + badge_name_length..];
        }

        self.refresh_badges().await?;

        let badge_rows = self
            .state
            .db
            .run_read_table(&format!(
                "SELECT COALESCE(NULLIF(badge, ''), badgeid),slotid FROM users_badges WHERE userid = '{}' ORDER BY slotid ASC",
                user_id
            ))
            .await?;
        let mut notify = format!("{}\u{2}{}", user_id, encode_vl64(enabled_badge_amount));
        let mut current_badges = std::array::from_fn::<_, 5, _>(|_| String::new());
        for row in badge_rows {
            if row.len() < 2 {
                continue;
            }
            let slot_id = row[1].parse::<i64>().unwrap_or(0);
            if slot_id > 0 {
                notify.push_str(&encode_vl64(slot_id as i32));
                notify.push_str(&row[0]);
                notify.push('\u{2}');
                if let Ok(index) = usize::try_from(slot_id - 1)
                    && index < current_badges.len()
                {
                    current_badges[index] = row[0].clone();
                }
            }
        }

        persist_current_badges(&self.state, user_id, &current_badges).await?;

        if let Some(mut room) = room_manager::get_room(&self.state, self.current_room_id).await {
            if let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) {
                room.users[index].badges = current_badges;
                room_manager::save_room(&self.state, room.clone()).await;
            }
            self.broadcast_room_users(&room, &format!("Cd{}", notify), None)
                .await;
        }

        Ok(())
    }

    async fn refresh_hand(&mut self, mode: &str) -> Result<()> {
        let user_id = self.user_id()?;
        let hand = build_hand_packet(&self.state, user_id, &mut self.hand_page, mode).await?;
        self.send(&hand)?;
        if let Some(user) = user_manager::get_user(&self.state, user_id).await {
            user.hand_page.store(self.hand_page, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn buy_game_tickets(&self, packet: &str) -> Result<()> {
        let user_id = self.user_id()?;
        let args = packet.get(2..).unwrap_or_default();
        if args.len() < 3 {
            return Ok(());
        }

        let amount = decode_vl64(&args[..3]).map(|(value, _)| value).unwrap_or(0);
        let receiver = {
            let parsed = args[3..].trim_matches('\u{2}').trim_matches('\0').trim();
            if parsed.is_empty() {
                self.username.clone().unwrap_or_default()
            } else {
                parsed.to_string()
            }
        };
        let Some((ticket_amount, price)) = game_ticket_offer(amount) else {
            return Ok(());
        };

        let credits = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;
        if price > credits {
            self.send("AD")?;
            return Ok(());
        }

        let receiver_id = user_manager::get_user_id(&self.state, &receiver).await;
        if receiver_id <= 0 {
            self.send(&format!("AL{}", receiver))?;
            return Ok(());
        }

        let new_credits = credits - price;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET credits = '{}' WHERE id = '{}' LIMIT 1",
                new_credits, user_id
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET tickets = tickets + {} WHERE id = '{}' LIMIT 1",
                ticket_amount, receiver_id
            ))
            .await?;
        self.send(&format!("@F{}", new_credits))?;

        if receiver_id == user_id {
            self.refresh_valueables(false, true).await?;
        } else if user_manager::contains_user_by_id(&self.state, receiver_id).await {
            mus_refresh_valueables(&self.state, receiver_id, true, true).await?;
        }

        Ok(())
    }

    async fn redeem_voucher(&self, packet: &str) -> Result<()> {
        let user_id = self.user_id()?;
        let code = Database::stripslash(packet.get(4..).unwrap_or_default());
        let exists = self
            .state
            .db
            .check_exists(&format!(
                "SELECT voucher FROM vouchers WHERE voucher = '{}' LIMIT 1",
                code
            ))
            .await;

        if !exists {
            self.send("CU1")?;
            return Ok(());
        }

        let voucher_amount = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM vouchers WHERE voucher = '{}' LIMIT 1",
                code
            ))
            .await;
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM vouchers WHERE voucher = '{}' LIMIT 1",
                code
            ))
            .await?;

        let new_credits = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await
            + voucher_amount;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET credits = '{}' WHERE id = '{}' LIMIT 1",
                new_credits, user_id
            ))
            .await?;

        self.send(&format!("@F{}", new_credits))?;
        self.send("CT")?;
        Ok(())
    }

    async fn catalogue_purchase(&mut self, packet: &str) -> Result<()> {
        let user_id = self.user_id()?;
        let packet_content: Vec<&str> = packet.split('\r').collect();
        if packet_content.len() < 6 {
            return Ok(());
        }

        let page = packet_content[1];
        let item = packet_content[3];
        let page_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT indexid FROM catalogue_pages WHERE indexname = '{}' AND minrank <= {} LIMIT 1",
                Database::stripslash(page),
                self.rank
            ))
            .await;
        let template_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tid FROM catalogue_items WHERE name_cct = '{}' LIMIT 1",
                Database::stripslash(item)
            ))
            .await;
        let cost = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT catalogue_cost FROM catalogue_items WHERE catalogue_id_page = '{}' AND tid = '{}' LIMIT 1",
                page_id, template_id
            ))
            .await;
        let credits = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;

        if cost == 0 || cost > credits {
            self.send("AD")?;
            return Ok(());
        }

        let mut receiver_id = user_id;
        let mut present_box_id = 0_i64;
        let mut room_id = 0_i64;

        if packet_content.get(5).copied().unwrap_or("0") == "1" {
            let receiver_name = packet_content.get(6).copied().unwrap_or_default();
            if receiver_name != self.username.clone().unwrap_or_default() {
                let found_id = user_manager::get_user_id(&self.state, receiver_name).await;
                if found_id > 0 {
                    receiver_id = found_id;
                } else {
                    self.send(&format!("AL{}", receiver_name))?;
                    return Ok(());
                }
            }

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as i64)
                .unwrap_or(1);
            let box_sprite = format!("present_gen{}", (nanos % 6) + 1);
            let box_template_id = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT tid FROM catalogue_items WHERE name_cct = '{}' LIMIT 1",
                    box_sprite
                ))
                .await;
            let box_note = Database::stripslash(
                &string_manager::filter_swearwords(
                    &self.state,
                    packet_content.get(7).copied().unwrap_or_default(),
                )
                .await,
            );
            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO furniture(tid,ownerid,var) VALUES ('{}','{}','!{}')",
                    box_template_id, receiver_id, box_note
                ))
                .await?;
            present_box_id = catalogue_manager::last_item_id(&self.state).await;
            room_id = -1;
        }

        let new_credits = credits - cost;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET credits = '{}' WHERE id = '{}' LIMIT 1",
                new_credits, user_id
            ))
            .await?;
        self.send(&format!("@F{}", new_credits))?;

        if string_manager::get_string_part(item, 0, 4) == "deal" {
            let deal_id = item.get(4..).unwrap_or("0").parse::<i64>().unwrap_or(0);
            let item_ids = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT tid FROM catalogue_deals WHERE id = '{}'",
                    deal_id
                ))
                .await
                .unwrap_or_default();
            let item_amounts = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT amount FROM catalogue_deals WHERE id = '{}'",
                    deal_id
                ))
                .await
                .unwrap_or_default();

            for (index, deal_template_id) in item_ids.iter().enumerate() {
                let amount = item_amounts.get(index).copied().unwrap_or(1);
                for _ in 0..amount {
                    self.state
                        .db
                        .run_query(&format!(
                            "INSERT INTO furniture(tid,ownerid,roomid) VALUES ('{}','{}','{}')",
                            deal_template_id, receiver_id, room_id
                        ))
                        .await?;
                    catalogue_manager::handle_purchase(
                        &self.state,
                        *deal_template_id,
                        receiver_id,
                        room_id,
                        "0",
                        present_box_id,
                    )
                    .await?;
                }
            }
        } else {
            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO furniture(tid,ownerid,roomid) VALUES ('{}','{}','{}')",
                    template_id, receiver_id, room_id
                ))
                .await?;

            let template = catalogue_manager::get_template(&self.state, template_id).await;
            if template.sprite == "wallpaper"
                || template.sprite == "floor"
                || template.sprite.contains("landscape")
            {
                let decor_id = packet_content.get(4).copied().unwrap_or("0");
                catalogue_manager::handle_purchase(
                    &self.state,
                    template_id,
                    receiver_id,
                    0,
                    decor_id,
                    present_box_id,
                )
                .await?;
            } else if string_manager::get_string_part(item, 0, 11) == "prizetrophy"
                || string_manager::get_string_part(item, 0, 11) == "greektrophy"
            {
                let inscription = packet_content.get(4).copied().unwrap_or_default();
                let item_variable = format!(
                    "{}\t{}\t{}",
                    self.username.clone().unwrap_or_default(),
                    chrono::Local::now().format("%d-%m-%Y"),
                    inscription
                );
                self.state
                    .db
                    .run_query(&format!(
                        "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                        Database::stripslash(&item_variable),
                        catalogue_manager::last_item_id(&self.state).await
                    ))
                    .await?;
                catalogue_manager::handle_purchase(
                    &self.state,
                    template_id,
                    receiver_id,
                    0,
                    "0",
                    present_box_id,
                )
                .await?;
            } else {
                catalogue_manager::handle_purchase(
                    &self.state,
                    template_id,
                    receiver_id,
                    room_id,
                    "0",
                    present_box_id,
                )
                .await?;
            }
        }

        if receiver_id == user_id {
            self.refresh_hand("last").await?;
        }

        Ok(())
    }

    async fn recycler_proceed(&mut self, packet: &str) -> Result<()> {
        let user_id = self.user_id()?;
        if recycler_manager::session_exists(&self.state, user_id).await {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (item_count, offset) = match decode_vl64(args) {
            Ok((value, used)) if value > 0 => (value as i64, used),
            _ => return Ok(()),
        };
        if !recycler_manager::reward_exists(&self.state, item_count).await {
            return Ok(());
        }

        recycler_manager::create_session(&self.state, user_id, item_count).await?;
        let mut cursor = offset;
        for _ in 0..item_count {
            let remaining = &args[cursor..];
            let (item_id, used) = match decode_vl64(remaining) {
                Ok((value, used)) if value > 0 => (value as i64, used),
                _ => {
                    recycler_manager::drop_session(&self.state, user_id, true).await?;
                    self.send("DpH")?;
                    return Ok(());
                }
            };

            let exists = self
                .state
                .db
                .check_exists(&format!(
                    "SELECT id FROM furniture WHERE id = '{}' AND ownerid = '{}' AND roomid = '0' LIMIT 1",
                    item_id, user_id
                ))
                .await;
            if !exists {
                recycler_manager::drop_session(&self.state, user_id, true).await?;
                self.send("DpH")?;
                return Ok(());
            }

            self.state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET roomid = '-2' WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await?;
            cursor += used;
        }

        self.send(&format!(
            "Dp{}",
            recycler_manager::session_string(&self.state, user_id).await
        ))?;
        self.refresh_hand("update").await?;
        Ok(())
    }

    async fn recycler_redeem_or_cancel(&mut self, packet: &str) -> Result<()> {
        let user_id = self.user_id()?;
        if !recycler_manager::session_exists(&self.state, user_id).await {
            return Ok(());
        }

        let redeem = packet.get(2..).unwrap_or_default() == "I";
        if redeem && recycler_manager::session_ready(&self.state, user_id).await {
            recycler_manager::reward_session(&self.state, user_id).await?;
        }
        recycler_manager::drop_session(&self.state, user_id, redeem).await?;

        self.send(&format!(
            "Dp{}",
            recycler_manager::session_string(&self.state, user_id).await
        ))?;
        if redeem {
            self.refresh_hand("last").await?;
        } else {
            self.refresh_hand("new").await?;
        }
        Ok(())
    }

    async fn enter_room_determine(&mut self, packet: &str) -> Result<()> {
        let args = packet.get(2..).unwrap_or_default();
        if args.is_empty() {
            return Ok(());
        }

        let is_publicroom = args.starts_with('A');
        let room_id_data = args.get(1..).unwrap_or_default();
        let room_id = match decode_vl64(room_id_data) {
            Ok((value, _)) if value > 0 => value as i64,
            _ => return Ok(()),
        };

        self.send("@S")?;
        // The original emulator emitted a legacy external tracking URL here. That endpoint now
        // triggers cross-domain/client-side noise and is not required for room entry, so the Rust
        // port suppresses it to preserve normal gameplay.

        if self.try_enter_game_arena(room_id, is_publicroom).await? {
            return Ok(());
        }
        if self.current_game_id.is_some() {
            self.game_lobby_leave().await?;
        } else if self.current_room_id > 0 && self.current_room_uid.is_some() {
            // Legacy Holograph removed the avatar from the current room as soon as a new @B
            // room-enter request started, even before target-room capacity/access checks.
            self.leave_room().await?;
        }

        let using_teleporter = self.pending_teleporter_id > 0;
        if !using_teleporter {
            let allow_enter_locked_rooms =
                rank_manager::contains_right(&self.state, self.rank, "fuse_enter_locked_rooms")
                    .await;
            let room_data = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT state,visitors_now,visitors_max FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            if room_data.len() >= 3 {
                let access_level = room_data[0].parse::<i32>().unwrap_or(0);
                if access_level == 3 && !self.is_club_member().await && !allow_enter_locked_rooms {
                    self.send("C`Kc")?;
                    return Ok(());
                }
                if access_level == 4 && !allow_enter_locked_rooms {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "room_stafflocked").await?
                    ))?;
                    return Ok(());
                }

                let now_visitors = room_data[1].parse::<i64>().unwrap_or(0);
                let max_visitors = room_data[2].parse::<i64>().unwrap_or(0);
                if now_visitors > 0
                    && max_visitors > 0
                    && now_visitors >= max_visitors
                    && !rank_manager::contains_right(
                        &self.state,
                        self.rank,
                        "fuse_enter_full_rooms",
                    )
                    .await
                {
                    if !is_publicroom {
                        self.send("C`I")?;
                    } else {
                        self.send(&format!(
                            "BK{}",
                            string_manager::get_string(&self.state, "room_full").await?
                        ))?;
                    }
                    return Ok(());
                }
            }
        }

        self.current_room_id = room_id;
        self.current_room_is_public = is_publicroom;
        self.room_access_primary_ok = true;
        self.room_access_secondary_ok = is_publicroom;
        self.is_owner = false;
        self.has_rights = false;

        if is_publicroom {
            let room_model = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT model FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await;
            if !room_model.is_empty() {
                self.send(&format!("AE{} {}", room_model, room_id))?;
            }
        }

        Ok(())
    }

    async fn enter_room_publicroom_advertisement(&self) -> Result<()> {
        if !self.current_room_is_public || self.current_room_id <= 0 {
            self.send("CP0")?;
            return Ok(());
        }

        if !self
            .state
            .db
            .check_exists(&format!(
                "SELECT roomid FROM room_ads WHERE roomid = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await
        {
            self.send("CP0")?;
            return Ok(());
        }

        let advert = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT img,uri FROM room_ads WHERE roomid = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await?;
        if advert.len() < 2 {
            self.send("CP0")?;
            return Ok(());
        }

        self.send(&format!("CP{}\t{}", advert[0], advert[1]))?;
        Ok(())
    }

    async fn enter_room_check_access(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_is_public {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let username = self.username.clone().unwrap_or_default();
        self.is_owner = self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                self.current_room_id,
                Database::stripslash(&username)
            ))
            .await;
        self.has_rights = self.is_owner
            || self
                .state
                .db
                .check_exists(&format!(
                    "SELECT userid FROM room_rights WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                    self.current_room_id, user_id
                ))
                .await
            || self
                .state
                .db
                .check_exists(&format!(
                    "SELECT id FROM rooms WHERE id = '{}' AND superusers = '1' LIMIT 1",
                    self.current_room_id
                ))
                .await;

        let access_flag = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT state FROM rooms WHERE id = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await as i32;

        if self.pending_teleporter_id > 0 && self.pending_teleporter_room_id == self.current_room_id
        {
            self.room_access_secondary_ok = true;
            self.send("@i")?;
            return Ok(());
        }

        if self
            .state
            .consume_doorbell_access(user_id, self.current_room_id)
            .await
        {
            self.room_access_secondary_ok = true;
            self.send("@i")?;
            return Ok(());
        }

        if self.is_owner || access_flag == 0 {
            self.room_access_secondary_ok = true;
            self.send("@i")?;
            return Ok(());
        }

        if self.current_game_id.is_none()
            && !self.is_owner
            && !rank_manager::contains_right(&self.state, self.rank, "fuse_enter_locked_rooms")
                .await
        {
            if !self.room_access_primary_ok && access_flag != 2 {
                return Ok(());
            }

            if self
                .state
                .db
                .check_exists(&format!(
                    "SELECT roomid FROM room_bans WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                    self.current_room_id, user_id
                ))
                .await
            {
                let ban_expire = self
                    .state
                    .db
                    .run_read_unsafe_string(&format!(
                        "SELECT ban_expire FROM room_bans WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                        self.current_room_id, user_id
                    ))
                    .await;
                let still_banned =
                    chrono::NaiveDateTime::parse_from_str(&ban_expire, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(&ban_expire, "%m/%d/%Y %H:%M:%S")
                        })
                        .map(|value| chrono::Local::now().naive_local() < value)
                        .unwrap_or(false);
                if still_banned {
                    self.send("C`PA")?;
                    self.send("@R")?;
                    self.reset_pending_room_access();
                    return Ok(());
                }

                self.state
                    .db
                    .run_query(&format!(
                        "DELETE FROM room_bans WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                        self.current_room_id, user_id
                    ))
                    .await?;
            }
        }

        if access_flag == 2 {
            let given_password = packet.get(2..).unwrap_or_default().trim();
            let room_password = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT password FROM rooms WHERE id = '{}' LIMIT 1",
                    self.current_room_id
                ))
                .await;
            if given_password == room_password {
                self.room_access_secondary_ok = true;
                self.send("@i")?;
            } else {
                self.send("@aIncorrect flat password")?;
            }
        } else if access_flag == 1 {
            if room_manager::get_room(&self.state, self.current_room_id)
                .await
                .is_none()
            {
                self.send("BC")?;
            } else {
                self.send_doorbell_to_room_rights(self.current_room_id, &format!("A[{}", username))
                    .await;
                self.send("A[")?;
            }
        } else {
            self.room_access_secondary_ok = true;
            self.send("@i")?;
        }

        Ok(())
    }

    async fn sync_current_room_permissions(&mut self) {
        if self.current_room_id <= 0 || self.current_room_is_public {
            return;
        }

        let Ok(user_id) = self.user_id() else {
            return;
        };
        let username = self.username.clone().unwrap_or_default();
        self.is_owner = self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                self.current_room_id,
                Database::stripslash(&username)
            ))
            .await;
        self.has_rights = self.is_owner
            || self
                .state
                .db
                .check_exists(&format!(
                    "SELECT userid FROM room_rights WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                    self.current_room_id, user_id
                ))
                .await
            || self
                .state
                .db
                .check_exists(&format!(
                    "SELECT id FROM rooms WHERE id = '{}' AND superusers = '1' LIMIT 1",
                    self.current_room_id
                ))
                .await;
    }

    fn reset_pending_room_access(&mut self) {
        self.current_room_id = 0;
        self.current_room_is_public = false;
        self.room_access_primary_ok = false;
        self.room_access_secondary_ok = false;
        self.is_owner = false;
        self.has_rights = false;
    }

    async fn reconcile_room_state_with_online_user(&mut self) -> Result<()> {
        if !self.has_local_room_state() {
            return Ok(());
        }
        let Some(user_id) = self.logged_in_user_id else {
            return Ok(());
        };
        let Some(user) = user_manager::get_user(&self.state, user_id).await else {
            return Ok(());
        };
        if !should_reconcile_room_state(self.current_room_uid, user.in_room, user.room_id) {
            return Ok(());
        }

        self.clear_room_access_markers_for_self().await?;
        self.current_room_uid = None;
        self.song_editor = None;
        self.reset_pending_room_access();
        Ok(())
    }

    fn has_local_room_state(&self) -> bool {
        self.current_room_id > 0
            || self.current_room_uid.is_some()
            || self.current_room_is_public
            || self.room_access_primary_ok
            || self.room_access_secondary_ok
            || self.is_owner
            || self.has_rights
    }

    async fn send_doorbell_to_room_rights(&self, room_id: i64, payload: &str) {
        let Some(room) = room_manager::get_room(&self.state, room_id).await else {
            return;
        };
        let owner_name = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT owner FROM rooms WHERE id = '{}' LIMIT 1",
                room_id
            ))
            .await;
        let rights_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT userid FROM room_rights WHERE roomid = '{}' ORDER BY userid ASC",
                room_id
            ))
            .await
            .unwrap_or_default();
        let room_superusers = self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND superusers = '1' LIMIT 1",
                room_id
            ))
            .await;

        for room_user in &room.users {
            let mut send = rights_ids.contains(&room_user.user_id);
            if !send && !owner_name.is_empty() {
                send = owner_name == room_user.username;
            }
            if !send && room_superusers {
                send = true;
            }
            if send && let Some(user) = user_manager::get_user(&self.state, room_user.user_id).await
            {
                let _ = user.sender.send(payload.to_string());
            }
        }
    }

    async fn consume_denied_doorbell_reset(&mut self) -> Result<bool> {
        let user_id = self.user_id()?;
        if self
            .state
            .consume_doorbell_denied(user_id, self.current_room_id)
            .await
        {
            // The C# server immediately cleared the denied ringer's pending room fields from the
            // owner session thread. Rust sessions do not expose direct mutable access, so we
            // consume a one-shot reset marker at the next room-entry packet boundary instead.
            self.reset_pending_room_access();
            return Ok(true);
        }

        Ok(false)
    }

    async fn clear_room_access_markers_for_self(&self) -> Result<()> {
        let user_id = self.user_id()?;
        self.state.clear_doorbell_access(user_id).await;
        self.state.clear_doorbell_denied(user_id).await;
        Ok(())
    }

    async fn enter_room_guestroom_data(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0
            || self.current_room_is_public
            || !self.room_access_secondary_ok
        {
            return Ok(());
        }

        let room_data = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT model,wallpaper,floor,landscape FROM rooms WHERE id = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await?;
        if room_data.len() < 4 {
            return Ok(());
        }

        self.send(&format!(
            "AEmodel_{} {}",
            room_data[0], self.current_room_id
        ))?;
        self.send(&format!("@nlandscape/{}", room_data[3]))?;
        if room_data[1].parse::<i32>().unwrap_or(0) > 0 {
            self.send(&format!("@nwallpaper/{}", room_data[1]))?;
        }
        if room_data[2].parse::<i32>().unwrap_or(0) > 0 {
            self.send(&format!("@nfloor/{}", room_data[2]))?;
        }

        if !self.is_owner
            && rank_manager::contains_right(&self.state, self.rank, "fuse_any_room_controller")
                .await
        {
            self.is_owner = true;
        }
        if self.is_owner {
            self.has_rights = true;
            self.send("@o")?;
        }
        if self.has_rights {
            self.send("@j")?;
        }

        let vote_amount = if self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM room_votes WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                self.user_id()?,
                self.current_room_id
            ))
            .await
        {
            self.state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT SUM(vote) FROM room_votes WHERE roomid = '{}' LIMIT 1",
                    self.current_room_id
                ))
                .await
                .max(0)
        } else {
            -1
        };

        self.send(&format!("EY{}", encode_vl64(vote_amount as i32)))?;
        self.hosts_event = self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM events WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                self.user_id()?,
                self.current_room_id
            ))
            .await;
        self.send(&format!(
            "Er{}",
            event_manager::get_event(&self.state, self.current_room_id).await
        ))?;
        Ok(())
    }

    async fn enter_room_load(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0 {
            return Ok(());
        }

        if !self.room_access_secondary_ok {
            if self.try_load_game_arena().await? {
                return Ok(());
            }
            return Ok(());
        }

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.send(&format!("@_{}", room.heightmap))?;
        self.send(&format!("@\\{}", room.dynamic_units()))?;
        Ok(())
    }

    async fn enter_room_items(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0 || !self.room_access_secondary_ok {
            return Ok(());
        }

        let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };
        self.send(&format!("@^{}", room.publicroom_items))?;
        self.send(&format!("@`{}", room.flooritems_legacy(&self.state).await))?;
        Ok(())
    }

    async fn enter_room_groups_and_lobby(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0 || !self.room_access_secondary_ok {
            return Ok(());
        }

        let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };
        self.send(&format!("Du{}", room.groups_legacy(&self.state).await))?;
        if let Some(ref lobby) = room.lobby {
            self.send(&format!(
                "CgH{}\u{2}{}{}",
                lobby.rank.title,
                encode_vl64(lobby.rank.min_points as i32),
                encode_vl64(lobby.rank.max_points as i32)
            ))?;
            self.send(&format!("Cz{}", room.lobby_player_ranks(&self.state).await))?;
        }
        self.send("DiH")?;
        if !self.received_sprite_index {
            self.send(&format!("Dg{}", SPRITE_INDEX))?;
            self.received_sprite_index = true;
        }
        Ok(())
    }

    async fn enter_room_wallitems(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0 || !self.room_access_secondary_ok {
            return Ok(());
        }

        let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };
        self.send(&format!("@m{}", room.wallitems_legacy(&self.state).await))?;
        Ok(())
    }

    async fn game_lobby_refresh(&mut self) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if let Some(lobby) = room.lobby {
            self.send(&format!("Ch{}", lobby.game_list()))?;
        }
        Ok(())
    }

    async fn room_rotate_user(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let mut parts = packet.get(2..).unwrap_or_default().split_whitespace();
        let Some(goal_x) = parts.next().and_then(|entry| entry.parse::<i32>().ok()) else {
            return Ok(());
        };
        let Some(goal_y) = parts.next().and_then(|entry| entry.parse::<i32>().ok()) else {
            return Ok(());
        };

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        if self.armed_tile_teleport || room.is_special_teleportable(user_id) {
            self.teleport_to_room_tile(&mut room, user_id, goal_x, goal_y)
                .await?;
            return Ok(());
        }
        let Some(status_packet) = room.rotate_user(user_id, goal_x, goal_y) else {
            return Ok(());
        };
        room_manager::save_room(&self.state, room.clone()).await;
        self.broadcast_room_users(&room, &status_packet, None).await;
        Ok(())
    }

    async fn room_walk_to_square(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        if args.len() < 4 {
            return Ok(());
        }

        let goal_x = i32::try_from(decode_b64(&args[..2]).unwrap_or(0)).unwrap_or(0);
        let goal_y = i32::try_from(decode_b64(&args[2..4]).unwrap_or(0)).unwrap_or(0);

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if room
            .users
            .iter()
            .find(|entry| entry.user_id == self.user_id().unwrap_or_default())
            .map(|entry| entry.walk_lock)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let user_id = self.user_id()?;
        if self.armed_tile_teleport || room.is_special_teleportable(user_id) {
            self.teleport_to_room_tile(&mut room, user_id, goal_x, goal_y)
                .await?;
            return Ok(());
        }
        room.set_user_goal(user_id, goal_x, goal_y);
        room_manager::save_room(&self.state, room).await;
        Ok(())
    }

    async fn room_click_door(&mut self) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if room
            .users
            .iter()
            .find(|entry| entry.user_id == self.user_id().unwrap_or_default())
            .map(|entry| entry.walk_door)
            .unwrap_or(false)
        {
            return Ok(());
        }
        room.set_user_door_goal(self.user_id()?);
        room_manager::save_room(&self.state, room).await;
        Ok(())
    }

    async fn teleport_to_room_tile(
        &mut self,
        room: &mut VirtualRoom,
        user_id: i64,
        goal_x: i32,
        goal_y: i32,
    ) -> Result<()> {
        self.armed_tile_teleport = false;
        if !room.teleport_user_to_tile(user_id, goal_x, goal_y) {
            room.set_special_teleportable(user_id, false);
            room_manager::save_room(&self.state, room.clone()).await;
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "scommand_failed").await?
            ))?;
            return Ok(());
        }

        let details_packet = room.user_details_packet(user_id);
        let status_packet = room.user_status_packet(user_id);
        room_manager::save_room(&self.state, room.clone()).await;
        if let Some(packet) = details_packet {
            self.broadcast_room_users(room, &packet, None).await;
        }
        if let Some(packet) = status_packet {
            self.broadcast_room_users(room, &packet, None).await;
        }
        Ok(())
    }

    async fn room_select_swim_outfit(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let outfit = parse_swim_outfit_selection(packet);

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if !room.has_swimming_pool {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        let Some(trigger) = room.get_trigger(room.users[index].x, room.users[index].y) else {
            return Ok(());
        };
        if trigger.object_name != "curtains1" && trigger.object_name != "curtains2" {
            return Ok(());
        }

        room.users[index].swim_outfit = outfit.clone();
        room.users[index].walk_lock = false;
        room.users[index].goal_x = trigger.goal_x;
        room.users[index].goal_y = trigger.goal_y;

        let details_packet = room.user_details_packet(user_id);
        room_manager::save_room(&self.state, room.clone()).await;

        if let Some(details_packet) = details_packet {
            self.broadcast_room_users(&room, &details_packet, None)
                .await;
        }
        self.broadcast_room_users(&room, &format!("AG{} open", trigger.object_name), None)
            .await;

        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET figure_swim = '{}' WHERE id = '{}' LIMIT 1",
                outfit, user_id
            ))
            .await?;
        self.refresh_appearance(true, true, true).await?;
        Ok(())
    }

    async fn room_place_item(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.has_rights {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let mut parts = packet.get(2..).unwrap_or_default().split_whitespace();
        let Some(item_id) = parts.next().and_then(|value| value.parse::<i64>().ok()) else {
            return Ok(());
        };

        let template_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tid FROM furniture WHERE id = '{}' AND ownerid = '{}' AND roomid = '0' LIMIT 1",
                item_id, user_id
            ))
            .await;
        if template_id == 0 {
            return Ok(());
        }

        let template = catalogue_manager::get_template(&self.state, template_id).await;
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;

        if template.type_id == 0 {
            let input_position = packet
                .get(2 + item_id.to_string().len() + 1..)
                .unwrap_or_default()
                .trim();
            let checked_position = catalogue_manager::wall_position_ok(input_position);
            if checked_position.is_empty() || checked_position != input_position {
                return Ok(());
            }

            let mut place_item_id = item_id;
            let mut var = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await;

            if template.sprite.starts_with("post.it") {
                if var.parse::<i32>().unwrap_or(0) > 1 {
                    self.state
                        .db
                        .run_query(&format!(
                            "UPDATE furniture SET var = var - 1 WHERE id = '{}' LIMIT 1",
                            item_id
                        ))
                        .await?;
                } else {
                    self.state
                        .db
                        .run_query(&format!(
                            "DELETE FROM furniture WHERE id = '{}' LIMIT 1",
                            item_id
                        ))
                        .await?;
                }
                self.state
                    .db
                    .run_query(&format!(
                        "INSERT INTO furniture(tid,ownerid) VALUES ('{}','{}')",
                        template_id, user_id
                    ))
                    .await?;
                place_item_id = catalogue_manager::last_item_id(&self.state).await;
                self.state
                    .db
                    .run_query(&format!(
                        "INSERT INTO furniture_stickies(id) VALUES ('{}')",
                        place_item_id
                    ))
                    .await?;
                var = "FFFF33".to_string();
                self.state
                    .db
                    .run_query(&format!(
                        "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                        var, place_item_id
                    ))
                    .await?;
            }

            if let Some(payload) = room
                .place_wall_item(
                    &self.state,
                    place_item_id,
                    template_id,
                    checked_position,
                    var,
                )
                .await?
            {
                room_manager::save_room(&self.state, room.clone()).await;
                self.broadcast_room_users(&room, &payload, None).await;
            }
        } else {
            let args: Vec<&str> = packet
                .get(2..)
                .unwrap_or_default()
                .split_whitespace()
                .collect();
            if args.len() < 4 {
                return Ok(());
            }
            let Some(x) = args.get(1).and_then(|value| value.parse::<i32>().ok()) else {
                return Ok(());
            };
            let Some(y) = args.get(2).and_then(|value| value.parse::<i32>().ok()) else {
                return Ok(());
            };
            let Some(z) = args.get(3).and_then(|value| value.parse::<i32>().ok()) else {
                return Ok(());
            };
            let var = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await;
            let max_stack_height = self.config_int("items_stacking_maxstackheight", 8).await as f64;
            let packets = room
                .place_floor_item(
                    &self.state,
                    item_id,
                    template_id,
                    x,
                    y,
                    z,
                    var,
                    max_stack_height,
                )
                .await?;
            if !packets.is_empty() {
                room_manager::save_room(&self.state, room.clone()).await;
                for payload in packets {
                    self.broadcast_room_users(&room, &payload, None).await;
                }
            }
        }

        Ok(())
    }

    async fn room_apply_decor(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.has_rights {
            return Ok(());
        }

        let payload = packet.get(2..).unwrap_or_default();
        let mut parts = payload.split('/');
        let decor_type = parts.next().unwrap_or_default();
        let item_id = parts
            .next()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if item_id <= 0 {
            return Ok(());
        }
        if decor_type != "wallpaper" && decor_type != "floor" && decor_type != "landscape" {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let template_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tid FROM furniture WHERE id = '{}' AND ownerid = '{}' AND roomid = '0' LIMIT 1",
                item_id, user_id
            ))
            .await;
        if template_id <= 0 {
            return Ok(());
        }

        let template = catalogue_manager::get_template(&self.state, template_id).await;
        if template.sprite != decor_type {
            return Ok(());
        }

        let decor_value = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;
        self.state
            .db
            .run_query(&format!(
                "UPDATE rooms SET {} = '{}' WHERE id = '{}' LIMIT 1",
                decor_type,
                Database::stripslash(&decor_value),
                self.current_room_id
            ))
            .await?;

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(&room, &format!("@n{}/{}", decor_type, decor_value), None)
            .await;

        self.state
            .db
            .run_query(&format!(
                "DELETE FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await?;
        Ok(())
    }

    async fn room_pickup_item(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.is_owner {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let Some(item_id) = packet
            .split_whitespace()
            .last()
            .and_then(|value| value.parse::<i64>().ok())
        else {
            return Ok(());
        };

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;

        if room.contains_floor_item(item_id) {
            let packets = room
                .remove_floor_item(&self.state, item_id, user_id)
                .await?;
            if !packets.is_empty() {
                room_manager::save_room(&self.state, room.clone()).await;
                for payload in packets {
                    self.broadcast_room_users(&room, &payload, None).await;
                }
                self.refresh_hand("update").await?;
            }
        } else if let Some(item) = room.wall_item(item_id) {
            let sprite = item.sprite(&self.state).await;
            if !sprite.starts_with("post.it")
                && let Some(payload) = room.remove_wall_item(&self.state, item_id, user_id).await?
            {
                room_manager::save_room(&self.state, room.clone()).await;
                self.broadcast_room_users(&room, &payload, None).await;
                self.refresh_hand("update").await?;
            }
        }

        Ok(())
    }

    async fn room_move_item(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.has_rights {
            return Ok(());
        }

        let args: Vec<&str> = packet
            .get(2..)
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        if args.len() < 4 {
            return Ok(());
        }

        let Some(item_id) = args.first().and_then(|value| value.parse::<i64>().ok()) else {
            return Ok(());
        };
        let Some(x) = args.get(1).and_then(|value| value.parse::<i32>().ok()) else {
            return Ok(());
        };
        let Some(y) = args.get(2).and_then(|value| value.parse::<i32>().ok()) else {
            return Ok(());
        };
        let Some(z) = args.get(3).and_then(|value| value.parse::<i32>().ok()) else {
            return Ok(());
        };

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let max_stack_height = self.config_int("items_stacking_maxstackheight", 8).await as f64;
        let packets = room
            .relocate_floor_item(&self.state, item_id, x, y, z, max_stack_height)
            .await?;
        if !packets.is_empty() {
            room_manager::save_room(&self.state, room.clone()).await;
            for payload in packets {
                self.broadcast_room_users(&room, &payload, None).await;
            }
        }
        Ok(())
    }

    async fn room_toggle_wall_item(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public {
            return Ok(());
        }
        if packet.len() < 4 {
            return Ok(());
        }

        let item_len = decode_b64(&packet[2..4]).unwrap_or(0);
        let Some(item_id) = packet
            .get(4..4 + item_len)
            .and_then(|value| value.parse::<i64>().ok())
        else {
            return Ok(());
        };
        let to_status = packet
            .get(4 + item_len..)
            .and_then(|data| decode_vl64(data).ok().map(|(value, _)| value))
            .unwrap_or(0);

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item) = room.wall_item(item_id) else {
            return Ok(());
        };
        let sprite = item.sprite(&self.state).await;
        if matches!(
            sprite.as_str(),
            "roomdimmer" | "post.it" | "post.it.vd" | "poster" | "habbowheel"
        ) {
            return Ok(());
        }

        if let Some(payload) = room
            .toggle_wall_item(&self.state, item_id, to_status)
            .await?
        {
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &payload, None).await;
        }
        Ok(())
    }

    async fn room_toggle_floor_item(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if packet.len() < 4 {
            return Ok(());
        }

        let item_len = decode_b64(&packet[2..4]).unwrap_or(0);
        let Some(item_id) = packet
            .get(4..4 + item_len)
            .and_then(|value| value.parse::<i64>().ok())
        else {
            return Ok(());
        };
        let to_status = Database::stripslash(packet.get(4 + item_len..).unwrap_or_default())
            .trim()
            .to_string();
        if to_status.is_empty() {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let packets = room
            .toggle_floor_item(&self.state, item_id, to_status, self.has_rights)
            .await?;
        if !packets.is_empty() {
            room_manager::save_room(&self.state, room.clone()).await;
            for payload in packets {
                self.broadcast_room_users(&room, &payload, None).await;
            }
        }
        Ok(())
    }

    async fn status_stop(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let status = packet.get(2..).unwrap_or_default();
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };

        match status {
            "CarryItem" => room.users[index].status_manager.drop_carryd_item(),
            "Dance" => room.users[index].status_manager.remove_status("dance"),
            _ => return Ok(()),
        }

        if let Some(status_packet) = room.user_status_packet(user_id) {
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &status_packet, None).await;
        } else {
            room_manager::save_room(&self.state, room).await;
        }
        Ok(())
    }

    async fn status_wave(&mut self) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        if room.users[index].status_manager.contains_status("wave") {
            return Ok(());
        }

        room.users[index].status_manager.remove_status("dance");
        room.users[index].status_manager.add_status("wave", "");
        let Some(status_packet) = room.user_status_packet(user_id) else {
            return Ok(());
        };
        room_manager::save_room(&self.state, room.clone()).await;
        self.broadcast_room_users(&room, &status_packet, None).await;

        let wave_duration = self.config_int("statuses_wave_duration", 1500).await;
        tokio::spawn(remove_status_after_delay(
            Arc::new((*self.state).clone()),
            self.current_room_id,
            user_id,
            "wave".to_string(),
            wave_duration.max(0) as u64,
        ));
        Ok(())
    }

    async fn status_dance(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        if room.users[index].status_manager.contains_status("sit")
            || room.users[index].status_manager.contains_status("lay")
        {
            return Ok(());
        }

        room.users[index].status_manager.drop_carryd_item();
        if packet.len() == 2 {
            room.users[index].status_manager.add_status("dance", "");
        } else {
            if !rank_manager::contains_right(&self.state, self.rank, "fuse_use_club_dance").await {
                return Ok(());
            }
            let dance_id = decode_vl64(packet.get(2..).unwrap_or_default())
                .map(|(value, _)| value)
                .unwrap_or(-1);
            if !(0..=4).contains(&dance_id) {
                return Ok(());
            }
            room.users[index]
                .status_manager
                .add_status("dance", &dance_id.to_string());
        }

        if let Some(status_packet) = room.user_status_packet(user_id) {
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &status_packet, None).await;
        } else {
            room_manager::save_room(&self.state, room).await;
        }
        Ok(())
    }

    async fn status_carry_item(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let item = packet.get(2..).unwrap_or_default().to_string();
        if item.is_empty() || item.contains('/') {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        if room.users[index].status_manager.contains_status("lay") {
            return Ok(());
        }

        if let Ok(number_item) = item.parse::<i32>() {
            if !(1..=26).contains(&number_item) {
                return Ok(());
            }
        } else if !self.current_room_is_public
            && item != "Water"
            && item != "Milk"
            && item != "Juice"
        {
            return Ok(());
        }

        room.users[index].status_manager.remove_status("dance");
        room.users[index].status_manager.drop_carryd_item();
        room.users[index].status_manager.add_status("carryd", &item);
        let Some(status_packet) = room.user_status_packet(user_id) else {
            return Ok(());
        };
        room_manager::save_room(&self.state, room.clone()).await;
        self.broadcast_room_users(&room, &status_packet, None).await;

        let sip_amount = self.config_int("statuses_carryitem_sipamount", 10).await;
        let sip_interval = self
            .config_int("statuses_carryitem_sipinterval", 9000)
            .await;
        let sip_duration = self
            .config_int("statuses_carryitem_sipduration", 1000)
            .await;
        tokio::spawn(run_carry_item_cycle(
            Arc::new((*self.state).clone()),
            self.current_room_id,
            user_id,
            item,
            sip_amount.max(0) as usize,
            sip_interval.max(0) as u64,
            sip_duration.max(0) as u64,
        ));
        Ok(())
    }

    async fn status_ignore_habbo(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        if room.users[index].status_manager.contains_status("sit")
            && room.users[index].status_manager.contains_status("lay")
        {
            room.users[index].status_manager.drop_carryd_item();
            if packet.len() == 2 {
                room.users[index].status_manager.add_status("ignore", "");
            }
            let status_packet = format!("@b{}", room.dynamic_statuses());
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &status_packet, None).await;
        }
        Ok(())
    }

    async fn status_listen_habbo(&self) -> Result<()> {
        Ok(())
    }

    async fn status_lido_vote(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        if room.users[index].status_manager.contains_status("sit")
            || room.users[index].status_manager.contains_status("lay")
        {
            return Ok(());
        }

        if packet.len() == 2 {
            room.users[index].status_manager.add_status("sign", "");
        } else {
            let sign_id = packet.get(2..).unwrap_or_default();
            room.users[index].status_manager.add_status("sign", sign_id);
            let wave_duration = self.config_int("statuses_wave_duration", 1500).await;
            tokio::spawn(remove_status_after_delay(
                Arc::new((*self.state).clone()),
                self.current_room_id,
                user_id,
                "sign".to_string(),
                wave_duration.max(0) as u64,
            ));
        }

        let status_packet = format!("@b{}", room.dynamic_statuses());
        room_manager::save_room(&self.state, room.clone()).await;
        self.broadcast_room_users(&room, &status_packet, None).await;
        Ok(())
    }

    async fn item_open_presentbox(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if !self.is_owner
            || self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match packet.get(2..).unwrap_or_default().trim().parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if !room.contains_floor_item(item_id) {
            return Ok(());
        }

        let item_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT itemid FROM furniture_presents WHERE id = '{}'",
                item_id
            ))
            .await
            .unwrap_or_default();
        for present_item_id in &item_ids {
            self.state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET roomid = '0' WHERE id = '{}' LIMIT 1",
                    present_item_id
                ))
                .await?;
        }

        let packets = room.remove_floor_item(&self.state, item_id, 0).await?;
        room_manager::save_room(&self.state, room.clone()).await;
        for payload in packets {
            self.broadcast_room_users(&room, &payload, None).await;
        }

        if let Some(last_item_id) = item_ids.last().copied() {
            let template_id = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT tid FROM furniture WHERE id = '{}' LIMIT 1",
                    last_item_id
                ))
                .await;
            let template = catalogue_manager::get_template(&self.state, template_id).await;
            if template.type_id > 0 {
                self.send(&format!(
                    "BA{}\r{}\r{}\u{1e}{}\u{1e}{}",
                    template.sprite,
                    template.sprite,
                    template.length,
                    template.width,
                    template.colour
                ))?;
            } else {
                self.send(&format!(
                    "BA{}\r{} {}\r",
                    template.sprite, template.sprite, template.colour
                ))?;
            }
        }

        self.state
            .db
            .run_query(&format!(
                "DELETE FROM furniture_presents WHERE id = '{}' LIMIT {}",
                item_id,
                item_ids.len()
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await?;
        self.refresh_hand("last").await?;
        Ok(())
    }

    async fn item_redeem_credit(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if !self.is_owner
            || self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if item_id <= 0 {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item) = room.floor_item(item_id).cloned() else {
            return Ok(());
        };
        let sprite = item.sprite(&self.state).await;
        let lowered = sprite.to_lowercase();
        if !lowered.starts_with("cf_") && !lowered.starts_with("cfc_") {
            return Ok(());
        }

        let redeem_value = match sprite
            .split('_')
            .nth(1)
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(value) if value > 0 => value,
            _ => return Ok(()),
        };

        let packets = room.remove_floor_item(&self.state, item_id, 0).await?;
        if !packets.is_empty() {
            room_manager::save_room(&self.state, room.clone()).await;
            for payload in packets {
                self.broadcast_room_users(&room, &payload, None).await;
            }
        }

        let user_id = self.user_id()?;
        let credits = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;
        let new_credits = credits + redeem_value;
        self.send(&format!("@F{}", new_credits))?;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET credits = '{}' WHERE id = '{}' LIMIT 1",
                new_credits, user_id
            ))
            .await?;
        Ok(())
    }

    async fn item_close_dice(&mut self, packet: &str) -> Result<()> {
        self.item_update_dice(packet, false).await
    }

    async fn item_spin_dice(&mut self, packet: &str) -> Result<()> {
        self.item_update_dice(packet, true).await
    }

    async fn item_update_dice(&mut self, packet: &str, spin: bool) -> Result<()> {
        if self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match packet.get(2..).unwrap_or_default().trim().parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        let user_id = self.user_id()?;
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(user_index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        let Some(item_index) = room
            .floor_items
            .iter()
            .position(|entry| entry.id == item_id)
        else {
            return Ok(());
        };
        let sprite = room.floor_items[item_index].sprite(&self.state).await;
        if sprite != "edice" && sprite != "edicehc" {
            return Ok(());
        }

        let user_x = room.users[user_index].x;
        let user_y = room.users[user_index].y;
        let item_x = room.floor_items[item_index].x;
        let item_y = room.floor_items[item_index].y;
        if (user_x - item_x).abs() > 1 || (user_y - item_y).abs() > 1 {
            return Ok(());
        }

        if spin {
            let rnd_num = ((chrono::Local::now().timestamp_subsec_millis() % 6) + 1) as i32;
            room.floor_items[item_index].var = rnd_num.to_string();
            self.state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                    rnd_num, item_id
                ))
                .await?;
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &format!("AZ{}", item_id), None)
                .await;
            tokio::spawn(broadcast_room_payload_after_delay(
                Arc::new((*self.state).clone()),
                self.current_room_id,
                format!("AZ{} {}", item_id, (item_id * 38) + i64::from(rnd_num)),
                2000,
            ));
        } else {
            room.floor_items[item_index].var = "0".to_string();
            self.state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '0' WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await?;
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &format!("AZ{} {}", item_id, item_id * 38), None)
                .await;
        }
        Ok(())
    }

    async fn item_open_sticky(&self, packet: &str) -> Result<()> {
        if self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match packet.get(2..).unwrap_or_default().trim().parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if room.wall_item(item_id).is_none() {
            return Ok(());
        }

        let message = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT text FROM furniture_stickies WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;
        let colour = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;
        self.send(&format!("@p{}\t{} {}", item_id, colour, message))?;
        Ok(())
    }

    async fn item_edit_sticky(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if !self.has_rights
            || self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let Some(split_index) = packet.find('/') else {
            return Ok(());
        };
        let item_id = match packet
            .get(2..split_index)
            .unwrap_or_default()
            .parse::<i64>()
        {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item_index) = room.wall_items.iter().position(|entry| entry.id == item_id) else {
            return Ok(());
        };
        let sprite = room.wall_items[item_index].sprite(&self.state).await;
        if sprite != "post.it" && sprite != "post.it.vd" {
            return Ok(());
        }

        let mut colour = "FFFFFF".to_string();
        if sprite == "post.it" {
            colour = packet
                .get(2 + item_id.to_string().len() + 1..2 + item_id.to_string().len() + 7)
                .unwrap_or_default()
                .to_string();
            if !matches!(colour.as_str(), "FFFF33" | "FF9CFF" | "9CFF9C" | "9CCEFF") {
                return Ok(());
            }
        }

        let raw_message = packet
            .get(2 + item_id.to_string().len() + 7..)
            .unwrap_or_default();
        if raw_message.len() > 684 {
            return Ok(());
        }
        if colour != room.wall_items[item_index].var {
            self.state
                .db
                .run_query(&format!(
                    "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                    colour, item_id
                ))
                .await?;
        }
        room.wall_items[item_index].var = colour.clone();
        room_manager::save_room(&self.state, room.clone()).await;
        self.broadcast_room_users(
            &room,
            &format!(
                "AU{}\t{}\t {}\t{}",
                item_id, sprite, room.wall_items[item_index].wall_position, colour
            ),
            None,
        )
        .await;

        let message = Database::stripslash(
            &string_manager::filter_swearwords(&self.state, raw_message)
                .await
                .replace("/r", "\r"),
        );
        self.state
            .db
            .run_query(&format!(
                "UPDATE furniture_stickies SET text = '{}' WHERE id = '{}' LIMIT 1",
                message, item_id
            ))
            .await?;
        Ok(())
    }

    async fn item_delete_sticky(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if !self.is_owner
            || self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match packet.get(2..).unwrap_or_default().trim().parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item_index) = room.wall_items.iter().position(|entry| entry.id == item_id) else {
            return Ok(());
        };
        let sprite = room.wall_items[item_index].sprite(&self.state).await;
        if !sprite.starts_with("post.it") {
            return Ok(());
        }

        if let Some(payload) = room.remove_wall_item(&self.state, item_id, 0).await? {
            room_manager::save_room(&self.state, room.clone()).await;
            self.broadcast_room_users(&room, &payload, None).await;
        }
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM furniture_stickies WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await?;
        Ok(())
    }

    async fn room_chat_say(&mut self, packet: &str, shout: bool) -> Result<()> {
        if self.current_room_id <= 0 && self.current_game_id.is_none() {
            return Ok(());
        }
        if self.user_is_muted().await {
            return Ok(());
        }

        let raw_message = packet.get(4..).unwrap_or_default();
        if raw_message.is_empty() {
            return Ok(());
        }

        user_manager::add_chat_message(
            &self.state,
            &self.username.clone().unwrap_or_default(),
            self.current_room_id,
            raw_message,
        )
        .await;
        let message = string_manager::filter_swearwords(&self.state, raw_message).await;
        if let Some(command) = message.strip_prefix(':') {
            if self.execute_speech_command(command).await? {
                self.stop_chat_typing_for_speech_command().await?;
                return Ok(());
            }

            if speech_command_name(command).is_some_and(is_known_speech_command_name) {
                self.stop_chat_typing_for_speech_command().await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_failed").await?
                ))?;
                return Ok(());
            }
        }

        if self.current_room_uid.is_none() && self.current_game_id.is_some() {
            self.game_chat_say(&message, shout).await?;
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let outcome = room.room_chat_say(user_id, &message, shout);
        if outcome.chat_packet.is_empty() {
            return Ok(());
        }

        room_manager::save_room(&self.state, room).await;

        if let Some(typing_packet) = outcome.typing_packet {
            for target_user_id in &outcome.recipient_ids {
                if let Some(user) = user_manager::get_user(&self.state, *target_user_id).await {
                    let _ = user.sender.send(typing_packet.clone());
                }
            }
        }
        if let Some(status_packet) = outcome.status_packet {
            for target_user_id in &outcome.recipient_ids {
                if let Some(user) = user_manager::get_user(&self.state, *target_user_id).await {
                    let _ = user.sender.send(status_packet.clone());
                }
            }
            tokio::spawn(remove_status_after_delay(
                Arc::new((*self.state).clone()),
                self.current_room_id,
                user_id,
                "gest".to_string(),
                5000,
            ));
        }
        for target_user_id in outcome.recipient_ids {
            if let Some(user) = user_manager::get_user(&self.state, target_user_id).await {
                let _ = user.sender.send(outcome.chat_packet.clone());
            }
        }
        for packet in outcome.bot_packets {
            let room = room_manager::load_room(
                &self.state,
                self.current_room_id,
                self.current_room_is_public,
            )
            .await?;
            self.broadcast_room_users(&room, &packet, None).await;
        }
        Ok(())
    }

    async fn game_chat_say(&self, message: &str, shout: bool) -> Result<()> {
        let Some(room_uid) = self.current_game_player_room_uid().await? else {
            return Ok(());
        };
        let chat_packet = format!(
            "Ei{}H\u{1}{}{}{}\u{2}",
            encode_vl64(room_uid as i32),
            if shout { "@Z" } else { "@X" },
            encode_vl64(room_uid as i32),
            message
        );
        self.broadcast_game_packet(&chat_packet).await?;
        Ok(())
    }

    async fn room_chat_whisper(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }
        if self.user_is_muted().await {
            return Ok(());
        }

        let rest = packet.get(4..).unwrap_or_default();
        let receiver = rest.split(' ').next().unwrap_or_default();
        let raw_message = packet.get(receiver.len() + 5..).unwrap_or_default();
        if raw_message.is_empty() {
            return Ok(());
        }

        if receiver.is_empty()
            && let Some(command) = raw_message.strip_prefix(':')
        {
            if self.execute_speech_command(command).await? {
                self.stop_chat_typing_for_speech_command().await?;
                return Ok(());
            }

            if speech_command_name(command).is_some_and(is_known_speech_command_name) {
                self.stop_chat_typing_for_speech_command().await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_failed").await?
                ))?;
                return Ok(());
            }
        }
        if receiver.is_empty() {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        user_manager::add_chat_message(
            &self.state,
            &self.username.clone().unwrap_or_default(),
            self.current_room_id,
            raw_message,
        )
        .await;
        let message = string_manager::filter_swearwords(&self.state, raw_message).await;
        let outcome = room.room_chat_whisper(user_id, receiver, &message);
        if outcome.whisper_packet.is_empty() {
            if outcome.typing_packet.is_some() {
                room_manager::save_room(&self.state, room).await;
            }
            return Ok(());
        }

        room_manager::save_room(&self.state, room).await;

        if let Some(typing_packet) = outcome.typing_packet {
            if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                let _ = user.sender.send(typing_packet.clone());
            }
            if let Some(target_user_id) = outcome.target_user_id
                && let Some(user) = user_manager::get_user(&self.state, target_user_id).await
            {
                let _ = user.sender.send(typing_packet);
            }
        }

        if let Some(user) = user_manager::get_user(&self.state, user_id).await {
            let _ = user.sender.send(outcome.whisper_packet.clone());
        }
        if let Some(target_user_id) = outcome.target_user_id
            && target_user_id != user_id
            && let Some(user) = user_manager::get_user(&self.state, target_user_id).await
        {
            let _ = user.sender.send(outcome.whisper_packet);
        }
        Ok(())
    }

    async fn room_poll_answer(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let mut remaining = packet.get(2..).unwrap_or_default();
        let (poll_id, used) = match decode_vl64(remaining) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        remaining = &remaining[used..];

        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT aid FROM poll_results WHERE uid = '{}' AND pid = '{}' LIMIT 1",
                user_id, poll_id
            ))
            .await
        {
            return Ok(());
        }

        let (question_id, used) = match decode_vl64(remaining) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        remaining = &remaining[used..];

        let type_three = self
            .state
            .db
            .check_exists(&format!(
                "SELECT type FROM poll_questions WHERE qid = '{}' AND type = '3' LIMIT 1",
                question_id
            ))
            .await;
        if type_three {
            if remaining.len() < 2 {
                return Ok(());
            }
            let count_answers = decode_b64(&remaining[..2]).unwrap_or(0);
            if remaining.len() < 2 + count_answers {
                return Ok(());
            }
            let answer = Database::stripslash(&remaining[2..2 + count_answers]);
            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO poll_results (pid,qid,aid,answers,uid) VALUES ('{}','{}','0','{}','{}')",
                    poll_id, question_id, answer, user_id
                ))
                .await?;
        } else {
            let (count_answers, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => return Ok(()),
            };
            remaining = &remaining[used..];
            for _ in 0..count_answers.max(0) {
                let (answer_id, used) = match decode_vl64(remaining) {
                    Ok(result) => result,
                    Err(_) => break,
                };
                remaining = &remaining[used..];
                self.state
                    .db
                    .run_query(&format!(
                        "INSERT INTO poll_results (pid,qid,aid,answers,uid) VALUES ('{}','{}','{}',' ','{}')",
                        poll_id, question_id, answer_id, user_id
                    ))
                    .await?;
            }
        }

        Ok(())
    }

    async fn current_sound_machine_id(&self) -> Result<i64> {
        if self.current_room_id <= 0 {
            return Ok(0);
        }
        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        Ok(room.sound_machine_id)
    }

    async fn sound_machine_initialize_song_list(&mut self) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if self.is_owner && machine_id > 0 {
            self.send(&format!(
                "EB{}",
                sound_machine_manager::get_machine_song_list(&self.state, machine_id).await
            ))?;
        }
        Ok(())
    }

    async fn sound_machine_initialize_playlist(&mut self) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if machine_id > 0 {
            self.send(&format!(
                "EC{}",
                sound_machine_manager::get_machine_playlist(&self.state, machine_id).await
            ))?;
        }
        Ok(())
    }

    async fn sound_machine_get_song(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if machine_id <= 0 {
            return Ok(());
        }
        let song_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        self.send(&format!(
            "Dl{}",
            sound_machine_manager::get_song(&self.state, song_id).await
        ))?;
        Ok(())
    }

    async fn sound_machine_save_playlist(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (amount, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        if amount >= 6 {
            return Ok(());
        }

        let mut remaining = &args[used..];
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM soundmachine_playlists WHERE machineid = '{}'",
                machine_id
            ))
            .await?;

        for pos in 0..amount {
            let (song_id, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => return Ok(()),
            };
            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO soundmachine_playlists(machineid,songid,pos) VALUES ('{}','{}','{}')",
                    machine_id,
                    song_id,
                    pos
                ))
                .await?;
            remaining = &remaining[used..];
        }

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(
            &room,
            &format!(
                "EC{}",
                sound_machine_manager::get_machine_playlist(&self.state, machine_id).await
            ),
            None,
        )
        .await;
        Ok(())
    }

    async fn sound_machine_burn_song_to_disk(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }

        let song_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let user_id = self.user_id()?;
        let credits = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;
        if credits <= 0
            || !self
                .state
                .db
                .check_exists(&format!(
                    "SELECT id FROM soundmachine_songs WHERE id = '{}' AND userid = '{}' AND machineid = '{}' LIMIT 1",
                    song_id,
                    user_id,
                    machine_id
                ))
                .await
        {
            self.send("AD")?;
            return Ok(());
        }

        let song_data = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT title,length FROM soundmachine_songs WHERE id = '{}' LIMIT 1",
                song_id
            ))
            .await?;
        if song_data.len() < 2 {
            self.send("AD")?;
            return Ok(());
        }

        let length = song_data[1].parse::<i64>().unwrap_or(0);
        let status = sound_machine_manager::build_burned_disk_status(
            song_id,
            self.username.as_deref().unwrap_or_default(),
            length,
            &song_data[0],
        );
        let disk_template_id = self.config_int("soundmachine_burntodisk_disktid", 0).await;
        self.state
            .db
            .run_query(&format!(
                "INSERT INTO furniture(tid,ownerid,var) VALUES ('{}','{}','{}')",
                disk_template_id,
                user_id,
                Database::stripslash(&status)
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "UPDATE soundmachine_songs SET burnt = '1' WHERE id = '{}' LIMIT 1",
                song_id
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET credits = credits - 1 WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await?;

        self.send(&format!("@F{}", credits - 1))?;
        self.send(&format!(
            "EB{}",
            sound_machine_manager::get_machine_song_list(&self.state, machine_id).await
        ))?;
        self.refresh_hand("last").await?;
        Ok(())
    }

    async fn sound_machine_delete_song(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }

        let song_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if !self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM soundmachine_songs WHERE id = '{}' AND machineid = '{}' LIMIT 1",
                song_id, machine_id
            ))
            .await
        {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "UPDATE soundmachine_songs SET machineid = '0' WHERE id = '{}' AND burnt = '1'",
                song_id
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM soundmachine_songs WHERE id = '{}' AND burnt = '0' LIMIT 1",
                song_id
            ))
            .await?;
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM soundmachine_playlists WHERE machineid = '{}' AND songid = '{}'",
                machine_id, song_id
            ))
            .await?;

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(
            &room,
            &format!(
                "EC{}",
                sound_machine_manager::get_machine_playlist(&self.state, machine_id).await
            ),
            None,
        )
        .await;
        Ok(())
    }

    async fn sound_machine_editor_initialize(&mut self) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }

        let mut song_editor = VirtualSongEditor::new(machine_id, self.user_id()?);
        song_editor.load_soundsets(&self.state).await;
        self.send(&format!("Dm{}", song_editor.get_soundsets()))?;
        self.send(&format!(
            "Dn{}",
            sound_machine_manager::get_hand_soundsets(&self.state, self.user_id()?).await
        ))?;
        self.song_editor = Some(song_editor);
        Ok(())
    }

    async fn sound_machine_editor_add_soundset(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }
        let user_id = self.user_id()?;
        let Some(song_editor) = self.song_editor.as_mut() else {
            return Ok(());
        };

        let args = packet.get(2..).unwrap_or_default();
        let (sound_set_id, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let slot_id = match decode_vl64(&args[used..]) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if !(1..5).contains(&slot_id) || !song_editor.slot_free(slot_id) {
            return Ok(());
        }

        song_editor
            .add_soundset(&self.state, sound_set_id as i64, slot_id)
            .await;
        let soundsets = song_editor.get_soundsets();
        let hand_soundsets = sound_machine_manager::get_hand_soundsets(&self.state, user_id).await;
        let hand_packet = format!("Dn{}", hand_soundsets);
        let soundsets_packet = format!("Dm{}", soundsets);
        let _ = song_editor;
        self.send(&hand_packet)?;
        self.send(&soundsets_packet)?;
        Ok(())
    }

    async fn sound_machine_editor_remove_soundset(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }
        let user_id = self.user_id()?;
        let Some(song_editor) = self.song_editor.as_mut() else {
            return Ok(());
        };

        let slot_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if song_editor.slot_free(slot_id) {
            return Ok(());
        }

        song_editor.remove_soundset(&self.state, slot_id).await;
        let soundsets = song_editor.get_soundsets();
        let hand_soundsets = sound_machine_manager::get_hand_soundsets(&self.state, user_id).await;
        let soundsets_packet = format!("Dm{}", soundsets);
        let hand_packet = format!("Dn{}", hand_soundsets);
        let _ = song_editor;
        self.send(&soundsets_packet)?;
        self.send(&hand_packet)?;
        Ok(())
    }

    async fn sound_machine_editor_save_new_song(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 || self.song_editor.is_none() {
            return Ok(());
        }
        if packet.len() < 6 {
            return Ok(());
        }

        let name_len = decode_b64(&packet[2..4]).unwrap_or(0);
        if packet.len() < name_len + 6 {
            return Ok(());
        }
        let title = &packet[4..4 + name_len];
        let data = &packet[name_len + 6..];
        let length = sound_machine_manager::calculate_song_length(data);
        if length == -1 {
            return Ok(());
        }

        let title =
            Database::stripslash(&string_manager::filter_swearwords(&self.state, title).await);
        let data = Database::stripslash(data);
        self.state
            .db
            .run_query(&format!(
                "INSERT INTO soundmachine_songs (userid,machineid,title,length,data) VALUES ('{}','{}','{}','{}','{}')",
                self.user_id()?,
                machine_id,
                title,
                length,
                Database::stripslash(&data)
            ))
            .await?;

        self.send(&format!(
            "EB{}",
            sound_machine_manager::get_machine_song_list(&self.state, machine_id).await
        ))?;
        self.send(&format!(
            "EK{}{}\u{2}",
            encode_vl64(machine_id as i32),
            title
        ))?;
        Ok(())
    }

    async fn sound_machine_editor_edit_song(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 {
            return Ok(());
        }

        let song_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        self.send(&format!(
            "Dl{}",
            sound_machine_manager::get_song(&self.state, song_id).await
        ))?;

        let mut song_editor = VirtualSongEditor::new(machine_id, self.user_id()?);
        song_editor.load_soundsets(&self.state).await;
        self.send(&format!("Dm{}", song_editor.get_soundsets()))?;
        self.send(&format!(
            "Dn{}",
            sound_machine_manager::get_hand_soundsets(&self.state, self.user_id()?).await
        ))?;
        self.song_editor = Some(song_editor);
        Ok(())
    }

    async fn sound_machine_editor_save_existing_song(&mut self, packet: &str) -> Result<()> {
        let machine_id = self.current_sound_machine_id().await?;
        if !self.is_owner || machine_id <= 0 || self.song_editor.is_none() {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (song_id, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        if !self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM soundmachine_songs WHERE id = '{}' AND userid = '{}' AND machineid = '{}' LIMIT 1",
                song_id,
                self.user_id()?,
                machine_id
            ))
            .await
        {
            return Ok(());
        }

        let args = &args[used..];
        if args.len() < 4 {
            return Ok(());
        }
        let name_len = decode_b64(&args[..2]).unwrap_or(0);
        if args.len() < name_len + 4 {
            return Ok(());
        }
        let title = &args[2..2 + name_len];
        let data = &args[name_len + 4..];
        let length = sound_machine_manager::calculate_song_length(data);
        if length == -1 {
            return Ok(());
        }

        let title =
            Database::stripslash(&string_manager::filter_swearwords(&self.state, title).await);
        let data = Database::stripslash(data);
        self.state
            .db
            .run_query(&format!(
                "UPDATE soundmachine_songs SET title = '{}',data = '{}',length = '{}' WHERE id = '{}' LIMIT 1",
                title,
                data,
                length,
                song_id
            ))
            .await?;

        self.send("ES")?;
        self.send(&format!(
            "EB{}",
            sound_machine_manager::get_machine_song_list(&self.state, machine_id).await
        ))?;
        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(
            &room,
            &format!(
                "EC{}",
                sound_machine_manager::get_machine_playlist(&self.state, machine_id).await
            ),
            None,
        )
        .await;
        Ok(())
    }

    async fn event_get_setup(&self) -> Result<()> {
        self.send(&format!(
            "Ep{}",
            encode_vl64(event_manager::category_amount(&self.state).await as i32)
        ))?;
        Ok(())
    }

    async fn event_host_button_visibility(&self) -> Result<()> {
        let hosts_event = if self.current_room_id > 0 {
            event_manager::user_hosts_any_event(&self.state, self.user_id().unwrap_or_default())
                .await
        } else {
            self.hosts_event
        };
        if self.current_room_is_public || self.current_room_uid.is_none() || hosts_event {
            self.send("EoH")?;
        } else {
            self.send("EoI")?;
        }
        Ok(())
    }

    async fn event_check_category(&self, packet: &str) -> Result<()> {
        let category_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if event_manager::category_ok(&self.state, category_id).await {
            self.send(&format!("Eb{}", encode_vl64(category_id as i32)))?;
        }
        Ok(())
    }

    async fn event_open_category(&self, packet: &str) -> Result<()> {
        let category_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if (1..=11).contains(&category_id) {
            self.send(&format!(
                "Eq{}{}",
                encode_vl64(category_id as i32),
                event_manager::get_events(&self.state, category_id).await
            ))?;
        }
        Ok(())
    }

    async fn event_create(&mut self, packet: &str) -> Result<()> {
        let already_hosts_event =
            event_manager::user_hosts_any_event(&self.state, self.user_id().unwrap_or_default())
                .await;
        if !self.is_owner
            || already_hosts_event
            || self.current_room_is_public
            || self.current_room_uid.is_none()
            || self.current_room_id <= 0
        {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (category_id, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let args = &args[used..];
        if args.len() < 4 || !event_manager::category_ok(&self.state, category_id as i64).await {
            return Ok(());
        }

        let name_len = decode_b64(&args[..2]).unwrap_or(0);
        if args.len() < name_len + 4 {
            return Ok(());
        }
        let name = &args[2..2 + name_len];
        let description = &args[name_len + 4..];

        self.hosts_event = true;
        event_manager::create_event(
            &self.state,
            category_id as i64,
            self.user_id()?,
            self.current_room_id,
            name,
            description,
        )
        .await?;

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(
            &room,
            &format!(
                "Er{}",
                event_manager::get_event(&self.state, self.current_room_id).await
            ),
            None,
        )
        .await;
        Ok(())
    }

    async fn event_edit(&mut self, packet: &str) -> Result<()> {
        let hosts_event_in_room = event_manager::user_hosts_event_in_room(
            &self.state,
            self.user_id().unwrap_or_default(),
            self.current_room_id,
        )
        .await;
        if !hosts_event_in_room
            || !self.is_owner
            || self.current_room_is_public
            || self.current_room_uid.is_none()
            || self.current_room_id <= 0
        {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (category_id, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let args = &args[used..];
        if args.len() < 4 || !event_manager::category_ok(&self.state, category_id as i64).await {
            return Ok(());
        }

        let name_len = decode_b64(&args[..2]).unwrap_or(0);
        if args.len() < name_len + 4 {
            return Ok(());
        }
        let name = &args[2..2 + name_len];
        let description = &args[name_len + 4..];

        event_manager::edit_event(
            &self.state,
            category_id as i64,
            self.current_room_id,
            name,
            description,
        )
        .await?;

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(
            &room,
            &format!(
                "Er{}",
                event_manager::get_event(&self.state, self.current_room_id).await
            ),
            None,
        )
        .await;
        Ok(())
    }

    async fn event_end(&mut self) -> Result<()> {
        let hosts_event_in_room = event_manager::user_hosts_event_in_room(
            &self.state,
            self.user_id().unwrap_or_default(),
            self.current_room_id,
        )
        .await;
        if !hosts_event_in_room
            || !self.is_owner
            || self.current_room_is_public
            || self.current_room_uid.is_none()
            || self.current_room_id <= 0
        {
            return Ok(());
        }

        self.hosts_event = false;
        event_manager::remove_event(&self.state, self.current_room_id).await?;

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(&room, "Er-1", None).await;
        Ok(())
    }

    async fn room_vote(&mut self, packet: &str) -> Result<()> {
        if self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let vote = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value,
            Err(_) => return Ok(()),
        };
        if vote != 1 && vote != -1 {
            return Ok(());
        }
        let user_id = self.user_id()?;
        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM room_votes WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                user_id, self.current_room_id
            ))
            .await
        {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "INSERT INTO room_votes (userid,roomid,vote) VALUES ('{}','{}','{}')",
                user_id, self.current_room_id, vote
            ))
            .await?;
        let vote_amount = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT SUM(vote) FROM room_votes WHERE roomid = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await
            .max(0);

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        room.mark_user_voted(user_id);
        let voted_user_ids = room.voted_user_ids();
        let room_event_packet = format!(
            "Er{}",
            event_manager::get_event(&self.state, self.current_room_id).await
        );
        room_manager::save_room(&self.state, room).await;

        if self.is_owner {
            let vote_packet = format!("EY{}", encode_vl64(vote_amount as i32));
            for target_user_id in voted_user_ids {
                if let Some(user) = user_manager::get_user(&self.state, target_user_id).await {
                    let _ = user.sender.send(vote_packet.clone());
                }
            }
        }

        self.send(&format!("EY{}", encode_vl64(vote_amount as i32)))?;
        self.send(&room_event_packet)?;
        Ok(())
    }

    async fn moodlight_toggle(&self) -> Result<()> {
        if self.current_room_id <= 0 || !self.is_owner {
            return Ok(());
        }

        let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };
        let Some(packet) = room_manager::moodlight::set_settings(
            &self.state,
            self.current_room_id,
            false,
            0,
            0,
            "",
            0,
        )
        .await?
        else {
            return Ok(());
        };

        self.broadcast_room_users(&room, &packet, None).await;
        Ok(())
    }

    async fn moodlight_load_settings(&self) -> Result<()> {
        if self.current_room_id <= 0 || !self.is_owner {
            return Ok(());
        }

        if let Some(setting_data) =
            room_manager::moodlight::get_settings(&self.state, self.current_room_id).await
        {
            self.send(&format!("Em{}", setting_data))?;
        }
        Ok(())
    }

    async fn moodlight_update(&self, packet: &str) -> Result<()> {
        if !self.has_rights
            || self.current_room_id <= 0
            || self.current_room_is_public
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };

        let packet = Database::stripslash(packet);
        let preset_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let bg_state = match decode_vl64(packet.get(3..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let colour_length = match packet.get(4..6) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        let preset_colour = match packet.get(6..6 + colour_length) {
            Some(value) => value,
            None => return Ok(()),
        };
        let preset_dark_f = match decode_vl64(packet.get(6 + colour_length..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };

        let Some(refresh_packet) = room_manager::moodlight::set_settings(
            &self.state,
            self.current_room_id,
            true,
            preset_id,
            bg_state,
            preset_colour,
            preset_dark_f,
        )
        .await?
        else {
            return Ok(());
        };

        if let Some(setting_data) =
            room_manager::moodlight::get_settings(&self.state, self.current_room_id).await
        {
            self.broadcast_room_users(&room, &format!("Em{}", setting_data), None)
                .await;
        }
        self.broadcast_room_users(&room, &refresh_packet, None)
            .await;
        Ok(())
    }

    async fn navigator_open_category(&self, packet: &str) -> Result<()> {
        if packet.len() < 4 {
            return Ok(());
        }

        let hide_full = match decode_vl64(packet.get(2..3).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let cata_id = match decode_vl64(packet.get(3..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };

        let name = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM room_categories WHERE id = '{}' AND (access_rank_min <= {} OR access_rank_hideforlower = '0') LIMIT 1",
                cata_id, self.rank
            ))
            .await;
        if name.is_empty() {
            return Ok(());
        }

        let category_type = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT type FROM room_categories WHERE id = '{}' LIMIT 1",
                cata_id
            ))
            .await;
        let parent_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT parent FROM room_categories WHERE id = '{}' LIMIT 1",
                cata_id
            ))
            .await;

        let order_helper = if category_type == 0 {
            if hide_full == 1 {
                "AND visitors_now < visitors_max ORDER BY id ASC".to_string()
            } else {
                "ORDER BY id ASC".to_string()
            }
        } else if hide_full == 1 {
            "AND visitors_now < visitors_max ORDER BY visitors_now DESC LIMIT 30".to_string()
        } else {
            format!(
                "ORDER BY visitors_now DESC LIMIT {}",
                self.config_int("navigator_opencategory_maxresults", 30)
                    .await
            )
        };

        let room_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM rooms WHERE category = '{}' {}",
                cata_id, order_helper
            ))
            .await
            .unwrap_or_default();

        let mut navigator = format!(
            "C\\{}{}{}{}\u{2}{}{}{}",
            encode_vl64(hide_full as i32),
            encode_vl64(cata_id as i32),
            encode_vl64(category_type as i32),
            name,
            encode_vl64(0),
            encode_vl64(10000),
            encode_vl64(parent_id as i32)
        );
        if category_type == 2 {
            navigator.push_str(&encode_vl64(room_ids.len() as i32));
        }

        if !room_ids.is_empty() {
            let room_states = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT state FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let show_name_flags = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT showname FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let now_visitors = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT visitors_now FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let max_visitors = self
                .state
                .db
                .run_read_column_i64(&format!(
                    "SELECT visitors_max FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let room_names = self
                .state
                .db
                .run_read_column_string(&format!(
                    "SELECT name FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let room_descriptions = self
                .state
                .db
                .run_read_column_string(&format!(
                    "SELECT description FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();
            let room_owners = self
                .state
                .db
                .run_read_column_string(&format!(
                    "SELECT owner FROM rooms WHERE category = '{}' {}",
                    cata_id, order_helper
                ))
                .await
                .unwrap_or_default();

            if category_type == 0 {
                let room_ccts = self
                    .state
                    .db
                    .run_read_column_string(&format!(
                        "SELECT ccts FROM rooms WHERE category = '{}' {}",
                        cata_id, order_helper
                    ))
                    .await
                    .unwrap_or_default();
                for i in 0..room_ids.len() {
                    navigator.push_str(&format!(
                        "{}{}{}\u{2}{}{}{}{}\u{2}{}{}{}\u{2}HI",
                        encode_vl64(room_ids[i] as i32),
                        encode_vl64(1),
                        room_names.get(i).cloned().unwrap_or_default(),
                        encode_vl64(now_visitors.get(i).copied().unwrap_or_default() as i32),
                        encode_vl64(max_visitors.get(i).copied().unwrap_or_default() as i32),
                        encode_vl64(cata_id as i32),
                        room_descriptions.get(i).cloned().unwrap_or_default(),
                        encode_vl64(room_ids[i] as i32),
                        encode_vl64(0),
                        room_ccts.get(i).cloned().unwrap_or_default(),
                    ));
                }
            } else {
                let can_see_hidden_names =
                    rank_manager::contains_right(&self.state, self.rank, "fuse_enter_locked_rooms")
                        .await;
                for i in 0..room_ids.len() {
                    if show_name_flags.get(i).copied().unwrap_or(1) == 0 && !can_see_hidden_names {
                        continue;
                    }
                    navigator.push_str(&format!(
                        "{}{}\u{2}{}\u{2}{}\u{2}{}{}{}\u{2}",
                        encode_vl64(room_ids[i] as i32),
                        room_names.get(i).cloned().unwrap_or_default(),
                        room_owners.get(i).cloned().unwrap_or_default(),
                        room_manager::room_state_name(
                            room_states.get(i).copied().unwrap_or_default() as i32
                        ),
                        encode_vl64(now_visitors.get(i).copied().unwrap_or_default() as i32),
                        encode_vl64(max_visitors.get(i).copied().unwrap_or_default() as i32),
                        room_descriptions.get(i).cloned().unwrap_or_default(),
                    ));
                }
            }
        }

        let sub_cata_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM room_categories WHERE parent = '{}' AND (access_rank_min <= {} OR access_rank_hideforlower = '0') ORDER BY id ASC",
                cata_id, self.rank
            ))
            .await
            .unwrap_or_default();
        for sub_cata_id in sub_cata_ids {
            let visitor_count = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT SUM(visitors_now) FROM rooms WHERE category = '{}' LIMIT 1",
                    sub_cata_id
                ))
                .await;
            let visitor_max = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT SUM(visitors_max) FROM rooms WHERE category = '{}' LIMIT 1",
                    sub_cata_id
                ))
                .await;
            if visitor_max > 0 && hide_full == 1 && visitor_count >= visitor_max {
                continue;
            }

            let sub_name = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT name FROM room_categories WHERE id = '{}' LIMIT 1",
                    sub_cata_id
                ))
                .await;
            navigator.push_str(&format!(
                "{}{}{}\u{2}{}{}{}",
                encode_vl64(sub_cata_id as i32),
                encode_vl64(0),
                sub_name,
                encode_vl64(visitor_count as i32),
                encode_vl64(visitor_max as i32),
                encode_vl64(cata_id as i32)
            ));
        }

        self.send(&navigator)?;
        Ok(())
    }

    async fn navigator_room_categories_for_placement(&self) -> Result<()> {
        let cata_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM room_categories WHERE type = '2' AND parent > 0 AND access_rank_min <= {} ORDER BY id ASC",
                self.rank
            ))
            .await
            .unwrap_or_default();
        let cata_names = self
            .state
            .db
            .run_read_column_string(&format!(
                "SELECT name FROM room_categories WHERE type = '2' AND parent > 0 AND access_rank_min <= {} ORDER BY id ASC",
                self.rank
            ))
            .await
            .unwrap_or_default();

        let mut categories = String::new();
        for i in 0..cata_ids.len() {
            categories.push_str(&encode_vl64(cata_ids[i] as i32));
            categories.push_str(cata_names.get(i).map(String::as_str).unwrap_or_default());
            categories.push('\u{2}');
        }
        self.send(&format!(
            "C]{}{}",
            encode_vl64(cata_ids.len() as i32),
            categories
        ))?;
        Ok(())
    }

    async fn navigator_recommended_rooms(&self) -> Result<()> {
        let mut rooms = String::new();
        for _ in 0..=3 {
            let room_details = self
                .state
                .db
                .run_read_row(
                    "SELECT id,name,owner,description,state,visitors_now,visitors_max FROM rooms WHERE NOT(owner IS NULL) ORDER BY RAND() LIMIT 1",
                )
                .await?;
            if room_details.is_empty() {
                return Ok(());
            }
            if room_details.len() >= 7 {
                rooms.push_str(&format!(
                    "{}{}\u{2}{}\u{2}{}\u{2}{}{}{}\u{2}",
                    encode_vl64(room_details[0].parse::<i32>().unwrap_or(0)),
                    room_details[1],
                    room_details[2],
                    room_manager::room_state_name(room_details[4].parse::<i32>().unwrap_or(0)),
                    encode_vl64(room_details[5].parse::<i32>().unwrap_or(0)),
                    encode_vl64(room_details[6].parse::<i32>().unwrap_or(0)),
                    room_details[3],
                ));
            }
        }
        self.send(&format!("E_{}{}", encode_vl64(3), rooms))?;
        Ok(())
    }

    async fn navigator_view_own_rooms(&self) -> Result<()> {
        let username = self.username.clone().unwrap_or_default();
        let room_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM rooms WHERE owner = '{}' ORDER BY id ASC",
                Database::stripslash(&username)
            ))
            .await
            .unwrap_or_default();
        if room_ids.is_empty() {
            self.send(&format!("@y{}", username))?;
            return Ok(());
        }

        let mut rooms = String::new();
        for room_id in room_ids {
            let room_details = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT name,description,state,showname,visitors_now,visitors_max FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            if room_details.len() < 6 {
                continue;
            }
            rooms.push_str(&format!(
                "{}\t{}\t{}\t{}\tx\t{}\t{}\tnull\t{}\t{}\t\r",
                room_id,
                room_details[0],
                username,
                room_manager::room_state_name(room_details[2].parse::<i32>().unwrap_or(0)),
                room_details[4],
                room_details[5],
                room_details[1],
                room_details[1]
            ));
        }
        self.send(&format!("@P{}", rooms))?;
        Ok(())
    }

    async fn navigator_room_search(&self, packet: &str) -> Result<()> {
        let see_all_room_owners =
            rank_manager::contains_right(&self.state, self.rank, "fuse_see_all_roomowners").await;
        let search = Database::stripslash(packet.get(2..).unwrap_or_default());
        let limit = self.config_int("navigator_roomsearch_maxresults", 30).await;
        let room_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM rooms WHERE NOT(owner IS NULL) AND (owner = '{}' OR name LIKE '%{}%') ORDER BY id ASC LIMIT {}",
                search, search, limit
            ))
            .await
            .unwrap_or_default();
        if room_ids.is_empty() {
            self.send("@z")?;
            return Ok(());
        }

        let mut rooms = String::new();
        let username = self.username.clone().unwrap_or_default();
        for room_id in room_ids {
            let mut room_details = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT name,owner,description,state,showname,visitors_now,visitors_max FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            if room_details.len() < 7 {
                continue;
            }
            if room_details[4] == "0" && room_details[1] != username && !see_all_room_owners {
                room_details[1] = "-".to_string();
            }
            rooms.push_str(&format!(
                "{}\t{}\t{}\t{}\tx\t{}\t{}\tnull\t{}\t\r",
                room_id,
                room_details[0],
                room_details[1],
                room_manager::room_state_name(room_details[3].parse::<i32>().unwrap_or(0)),
                room_details[5],
                room_details[6],
                room_details[2]
            ));
        }
        self.send(&format!("@w{}", rooms))?;
        Ok(())
    }

    async fn navigator_room_details(&self, packet: &str) -> Result<()> {
        let room_id = packet
            .get(2..)
            .unwrap_or_default()
            .parse::<i64>()
            .unwrap_or(0);
        if room_id <= 0 {
            return Ok(());
        }

        let room_details = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT name,owner,description,model,state,superusers,showname,category,visitors_now,visitors_max FROM rooms WHERE id = '{}' AND NOT(owner IS NULL) LIMIT 1",
                room_id
            ))
            .await?;
        if room_details.len() < 10 {
            return Ok(());
        }

        let mut details = format!(
            "{}{}{}",
            encode_vl64(room_details[5].parse::<i32>().unwrap_or(0)),
            encode_vl64(room_details[4].parse::<i32>().unwrap_or(0)),
            encode_vl64(room_id as i32)
        );
        if room_details[6] == "0"
            && rank_manager::contains_right(&self.state, self.rank, "fuse_see_all_roomowners").await
        {
            // Preserve the original Holograph condition here, even though the old comment contradicts it.
            details.push('-');
        } else {
            details.push_str(&room_details[1]);
        }

        details.push_str(&format!(
            "\u{2}model_{}\u{2}{}\u{2}{}\u{2}{}",
            room_details[3],
            room_details[0],
            room_details[2],
            encode_vl64(room_details[6].parse::<i32>().unwrap_or(0))
        ));
        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM room_categories WHERE id = '{}' AND trading = '1' LIMIT 1",
                room_details[7]
            ))
            .await
        {
            details.push('I');
        } else {
            details.push('H');
        }
        details.push_str(&format!(
            "{}{}",
            encode_vl64(room_details[8].parse::<i32>().unwrap_or(0)),
            encode_vl64(room_details[9].parse::<i32>().unwrap_or(0))
        ));
        self.send(&format!("@v{}", details))?;
        Ok(())
    }

    async fn navigator_favourite_rooms(&self) -> Result<()> {
        let user_id = self.user_id()?;
        let max_rooms = self.config_int("navigator_favourites_maxrooms", 30).await;
        let room_ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT roomid FROM users_favouriterooms WHERE userid = '{}' ORDER BY roomid DESC LIMIT {}",
                user_id, max_rooms
            ))
            .await
            .unwrap_or_default();
        if room_ids.is_empty() {
            return Ok(());
        }

        let mut deleted_amount = 0_i32;
        let mut guest_room_amount = 0_i32;
        let see_hidden_room_owners =
            rank_manager::contains_right(&self.state, self.rank, "fuse_enter_locked_rooms").await;
        let username = self.username.clone().unwrap_or_default();
        let mut rooms = String::new();

        for room_id in room_ids {
            let mut room_data = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT name,owner,state,showname,visitors_now,visitors_max,description FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            if room_data.is_empty() {
                if guest_room_amount > 0 {
                    deleted_amount += 1;
                }
                self.state
                    .db
                    .run_query(&format!(
                        "DELETE FROM users_favouriterooms WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                        user_id, room_id
                    ))
                    .await?;
                continue;
            }
            if room_data.len() < 7 {
                continue;
            }

            if room_data[1].is_empty() {
                let category_id = self
                    .state
                    .db
                    .run_read_unsafe_i64(&format!(
                        "SELECT category FROM rooms WHERE id = '{}' LIMIT 1",
                        room_id
                    ))
                    .await;
                let ccts = self
                    .state
                    .db
                    .run_read_unsafe_string(&format!(
                        "SELECT ccts FROM rooms WHERE id = '{}' LIMIT 1",
                        room_id
                    ))
                    .await;
                rooms.push_str(&format!(
                    "{}I{}\u{2}{}{}{}{}\u{2}{}H{}\u{2}HI",
                    encode_vl64(room_id as i32),
                    room_data[0],
                    encode_vl64(room_data[4].parse::<i32>().unwrap_or(0)),
                    encode_vl64(room_data[5].parse::<i32>().unwrap_or(0)),
                    encode_vl64(category_id as i32),
                    room_data[6],
                    encode_vl64(room_id as i32),
                    ccts
                ));
            } else {
                if room_data[3] == "0" && username != room_data[1] && !see_hidden_room_owners {
                    room_data[1] = "-".to_string();
                }
                rooms.push_str(&format!(
                    "{}{}\u{2}{}\u{2}{}\u{2}{}{}{}\u{2}",
                    encode_vl64(room_id as i32),
                    room_data[0],
                    room_data[1],
                    room_manager::room_state_name(room_data[2].parse::<i32>().unwrap_or(0)),
                    encode_vl64(room_data[4].parse::<i32>().unwrap_or(0)),
                    encode_vl64(room_data[5].parse::<i32>().unwrap_or(0)),
                    room_data[6],
                ));
                guest_room_amount += 1;
            }
        }

        self.send(&format!(
            "@}}HHJ\u{2}HHH{}{}",
            encode_vl64(guest_room_amount - deleted_amount),
            rooms
        ))?;
        Ok(())
    }

    async fn navigator_add_favourite(&self, packet: &str) -> Result<()> {
        let room_id = match decode_vl64(packet.get(3..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let user_id = self.user_id()?;
        if self
            .state
            .db
            .check_exists(&format!("SELECT id FROM rooms WHERE id = '{}' LIMIT 1", room_id))
            .await
            && !self
                .state
                .db
                .check_exists(&format!(
                    "SELECT userid FROM users_favouriterooms WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                    user_id, room_id
                ))
                .await
        {
            let count = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT COUNT(userid) FROM users_favouriterooms WHERE userid = '{}' LIMIT 1",
                    user_id
                ))
                .await;
            if count < self.config_int("navigator_favourites_maxrooms", 30).await {
                self.state
                    .db
                    .run_query(&format!(
                        "INSERT INTO users_favouriterooms(userid,roomid) VALUES ('{}','{}')",
                        user_id, room_id
                    ))
                    .await?;
            } else {
                self.send("@anav_error_toomanyfavrooms")?;
            }
        }
        Ok(())
    }

    async fn navigator_remove_favourite(&self, packet: &str) -> Result<()> {
        let room_id = match decode_vl64(packet.get(3..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM users_favouriterooms WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                self.user_id()?,
                room_id
            ))
            .await?;
        Ok(())
    }

    async fn guestroom_create_phase_one(&self, packet: &str) -> Result<()> {
        let room_settings = packet.split('/').collect::<Vec<_>>();
        if room_settings.len() < 6 {
            return Ok(());
        }

        let username = self.username.clone().unwrap_or_default();
        let owned_count = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT COUNT(id) FROM rooms WHERE owner = '{}' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await;
        if owned_count >= self.config_int("navigator_createroom_maxrooms", 25).await {
            self.send("@aError creating a private room")?;
            return Ok(());
        }

        let room_name = string_manager::filter_swearwords(&self.state, room_settings[2]).await;
        let room_name = Database::stripslash(&room_name);
        let model = room_settings[3].get(6..7).unwrap_or_default().to_string();
        let room_state = room_manager::room_state_id(room_settings[4]).to_string();
        if room_settings[5] != "0" && room_settings[5] != "1" {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "INSERT INTO rooms (name,owner,model,state,showname) VALUES ('{}','{}','{}','{}','{}')",
                room_name,
                Database::stripslash(&username),
                model,
                room_state,
                room_settings[5]
            ))
            .await?;
        let room_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT MAX(id) FROM rooms WHERE owner = '{}' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await;
        self.send(&format!("@{{{}\r{}", room_id, room_name))?;
        Ok(())
    }

    async fn guestroom_modify_details(&self, packet: &str) -> Result<()> {
        let room_id = if packet.get(2..3).unwrap_or_default() == "/" {
            packet
                .split('/')
                .nth(1)
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0)
        } else {
            packet
                .get(2..)
                .unwrap_or_default()
                .split('/')
                .next()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0)
        };
        if room_id <= 0 {
            return Ok(());
        }

        let mut super_users = 0_i64;
        let mut max_visitors = 25_i64;
        let packet_content = packet.split('\r').collect::<Vec<_>>();
        let mut room_description = String::new();
        let mut room_password = String::new();

        for entry in packet_content.iter().skip(1) {
            let Some((upd_header, upd_value)) = entry.split_once('=') else {
                return Ok(());
            };
            match upd_header {
                "description" => {
                    room_description = Database::stripslash(
                        &string_manager::filter_swearwords(&self.state, upd_value).await,
                    );
                }
                "allsuperuser" => {
                    super_users = upd_value.parse::<i64>().unwrap_or(0);
                    if super_users != 0 && super_users != 1 {
                        super_users = 0;
                    }
                }
                "maxvisitors" => {
                    max_visitors = upd_value.parse::<i64>().unwrap_or(25);
                    if !(10..=25).contains(&max_visitors) {
                        max_visitors = 25;
                    }
                }
                "password" => {
                    room_password = Database::stripslash(upd_value);
                }
                _ => return Ok(()),
            }
        }

        self.state
            .db
            .run_query(&format!(
                "UPDATE rooms SET description = '{}',superusers = '{}',visitors_max = '{}',password = '{}' WHERE id = '{}' AND owner = '{}' LIMIT 1",
                room_description,
                super_users,
                max_visitors,
                room_password,
                room_id,
                Database::stripslash(&self.username.clone().unwrap_or_default())
            ))
            .await?;
        Ok(())
    }

    async fn guestroom_modify_core(&self, packet: &str) -> Result<()> {
        let packet_content = packet
            .get(2..)
            .unwrap_or_default()
            .split('/')
            .collect::<Vec<_>>();
        if packet_content.len() < 3 {
            return Ok(());
        }
        let room_id = packet_content[0].parse::<i64>().unwrap_or(0);
        if room_id <= 0 {
            return Ok(());
        }

        let room_name = Database::stripslash(
            &string_manager::filter_swearwords(&self.state, packet_content[1]).await,
        );
        let mut show_name = packet_content[2].to_string();
        if show_name != "1" && show_name != "0" {
            show_name = "1".to_string();
        }
        // Preserve the original Holograph quirk: it derived room state from packetContent[2] too.
        let room_state = room_manager::room_state_id(packet_content[2]);

        self.state
            .db
            .run_query(&format!(
                "UPDATE rooms SET name = '{}',state = '{}',showname = '{}' WHERE id = '{}' AND owner = '{}' LIMIT 1",
                room_name,
                room_state,
                show_name,
                room_id,
                Database::stripslash(&self.username.clone().unwrap_or_default())
            ))
            .await?;
        Ok(())
    }

    async fn guestroom_modify_trigger(&self, packet: &str) -> Result<()> {
        let room_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let room_category = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT category FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                room_id,
                Database::stripslash(&self.username.clone().unwrap_or_default())
            ))
            .await;
        if !room_category.is_empty() {
            self.send(&format!(
                "C^{}{}",
                encode_vl64(room_id as i32),
                encode_vl64(room_category.parse::<i32>().unwrap_or(0))
            ))?;
        }
        Ok(())
    }

    async fn guestroom_modify_category(&self, packet: &str) -> Result<()> {
        let args = packet.get(2..).unwrap_or_default();
        let (room_id, used) = match decode_vl64(args) {
            Ok(result) => (result.0 as i64, result.1),
            Err(_) => return Ok(()),
        };
        let cata_id = match decode_vl64(&args[used..]) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM room_categories WHERE id = '{}' AND type = '2' AND parent > 0 AND access_rank_min <= {} LIMIT 1",
                cata_id, self.rank
            ))
            .await
        {
            self.state
                .db
                .run_query(&format!(
                    "UPDATE rooms SET category = '{}' WHERE id = '{}' AND owner = '{}' LIMIT 1",
                    cata_id,
                    room_id,
                    Database::stripslash(&self.username.clone().unwrap_or_default())
                ))
                .await?;
        }
        Ok(())
    }

    async fn guestroom_delete(&self, packet: &str) -> Result<()> {
        let room_id = packet
            .get(2..)
            .unwrap_or_default()
            .parse::<i64>()
            .unwrap_or(0);
        if room_id <= 0 {
            return Ok(());
        }

        let username = self.username.clone().unwrap_or_default();
        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                room_id,
                Database::stripslash(&username)
            ))
            .await
        {
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM room_rights WHERE roomid = '{}'",
                    room_id
                ))
                .await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM room_votes WHERE roomid = '{}'",
                    room_id
                ))
                .await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM room_bans WHERE roomid = '{}' LIMIT 1",
                    room_id
                ))
                .await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM furniture WHERE roomid = '{}'",
                    room_id
                ))
                .await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM furniture_moodlight WHERE roomid = '{}'",
                    room_id
                ))
                .await?;
        }

        if room_manager::contains_room(&self.state, room_id).await
            && let Some(room) = room_manager::get_room(&self.state, room_id).await
        {
            for room_user in room.users {
                if let Some(user) = user_manager::get_user(&self.state, room_user.user_id).await {
                    let _ = user.sender.send("@R".to_string());
                    let _ = user
                        .sender
                        .send("B!This room has been deleted\u{2}holo.cast.modkick".to_string());
                }
            }
            let _ = room_manager::remove_room(&self.state, room_id).await;
        }
        Ok(())
    }

    async fn navigator_publicroom_userlist(&self, packet: &str) -> Result<()> {
        let room_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if let Some(room) = room_manager::get_room(&self.state, room_id).await {
            self.send(&format!("C_{}", room.user_list_legacy()))?;
        } else {
            self.send("C_")?;
        }
        Ok(())
    }

    async fn messenger_request_friend(&self, packet: &str) -> Result<()> {
        let username = Database::stripslash(packet.get(4..).unwrap_or_default());
        let to_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT id FROM users WHERE name = '{}' LIMIT 1",
                username
            ))
            .await;
        if to_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let friend_ids = user_manager::get_user_friend_ids(&self.state, user_id).await;
        if friend_ids.contains(&to_id) {
            return Ok(());
        }
        let request_exists = self
            .state
            .db
            .check_exists(&format!(
                "SELECT requestid FROM messenger_friendrequests WHERE ((userid_to = '{}' AND userid_from = '{}') OR (userid_to = '{}' AND userid_from = '{}')) LIMIT 1",
                to_id, user_id, user_id, to_id
            ))
            .await;
        if request_exists {
            return Ok(());
        }

        let request_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT MAX(requestid) FROM messenger_friendrequests WHERE userid_to = '{}' LIMIT 1",
                to_id
            ))
            .await
            + 1;
        self.state
            .db
            .run_query(&format!(
                "INSERT INTO messenger_friendrequests(userid_to,userid_from,requestid) VALUES ('{}','{}','{}')",
                to_id, user_id, request_id
            ))
            .await?;

        if let Some(target) = user_manager::get_user(&self.state, to_id).await {
            let _ = target.sender.send(format!(
                "BDI{}\u{2}{}\u{2}",
                self.username.clone().unwrap_or_default(),
                user_id
            ));
        }
        Ok(())
    }

    async fn messenger_accept_requests(&self, packet: &str) -> Result<()> {
        let args = packet.get(2..).unwrap_or_default();
        let (amount, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let mut remaining = &args[used..];
        let user_id = self.user_id()?;
        let mut update_amount = 0_i32;
        let mut updates = String::new();

        for _ in 0..amount {
            if remaining.is_empty() {
                return Ok(());
            }
            let (request_id, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => return Ok(()),
            };
            let from_user_id = self
                .state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT userid_from FROM messenger_friendrequests WHERE userid_to = '{}' AND requestid = '{}' LIMIT 1",
                    user_id, request_id
                ))
                .await;
            if from_user_id == 0 {
                return Ok(());
            }

            updates.push_str(
                &crate::messenger::virtual_buddy::to_legacy_string(
                    &self.state,
                    from_user_id,
                    false,
                )
                .await?,
            );
            update_amount += 1;

            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO messenger_friendships(userid,friendid) VALUES ('{}','{}')",
                    from_user_id, user_id
                ))
                .await?;
            virtual_messenger::notify_buddy_added(&self.state, user_id, from_user_id).await?;
            virtual_messenger::notify_buddy_added(&self.state, from_user_id, user_id).await?;
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM messenger_friendrequests WHERE userid_to = '{}' AND requestid = '{}' LIMIT 1",
                    user_id, request_id
                ))
                .await?;

            remaining = &remaining[used..];
        }

        if update_amount > 0 {
            self.send(&format!("@MHH{}{}", encode_vl64(update_amount), updates))?;
        }
        Ok(())
    }

    async fn messenger_decline_requests(&self, packet: &str) -> Result<()> {
        let args = packet.get(2..).unwrap_or_default();
        let (amount, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let mut remaining = &args[used..];
        let user_id = self.user_id()?;
        for _ in 0..amount {
            if remaining.is_empty() {
                return Ok(());
            }
            let (request_id, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => return Ok(()),
            };
            self.state
                .db
                .run_query(&format!(
                    "DELETE FROM messenger_friendrequests WHERE userid_to = '{}' AND requestid = '{}' LIMIT 1",
                    user_id, request_id
                ))
                .await?;
            remaining = &remaining[used..];
        }
        Ok(())
    }

    async fn messenger_remove_buddy(&self, packet: &str) -> Result<()> {
        let buddy_id = match decode_vl64(packet.get(3..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM messenger_friendships WHERE (userid = '{}' AND friendid = '{}') OR (userid = '{}' AND friendid = '{}') LIMIT 1",
                self.user_id()?,
                buddy_id,
                buddy_id,
                self.user_id()?
            ))
            .await?;
        virtual_messenger::notify_buddy_removed(&self.state, self.user_id()?, buddy_id).await;
        virtual_messenger::notify_buddy_removed(&self.state, buddy_id, self.user_id()?).await;
        Ok(())
    }

    async fn messenger_instant_message(&self, packet: &str) -> Result<()> {
        let args = packet.get(2..).unwrap_or_default();
        let (buddy_id, used) = match decode_vl64(args) {
            Ok(result) => (result.0 as i64, result.1),
            Err(_) => return Ok(()),
        };
        let mut message = args.get(used + 2..).unwrap_or_default().to_string();
        message = string_manager::filter_swearwords(&self.state, &message).await;

        let friend_ids = user_manager::get_user_friend_ids(&self.state, self.user_id()?).await;
        if friend_ids.contains(&buddy_id)
            && let Some(buddy) = user_manager::get_user(&self.state, buddy_id).await
        {
            let _ = buddy.sender.send(format!(
                "BF{}{}\u{2}",
                encode_vl64(self.user_id()? as i32),
                message
            ));
        } else {
            self.send(&format!(
                "DE{}{}",
                encode_vl64(5),
                encode_vl64(self.user_id()? as i32)
            ))?;
        }
        Ok(())
    }

    async fn messenger_refresh_updates(&mut self) -> Result<()> {
        let user_id = self.user_id()?;
        let updates = virtual_messenger::build_updates_packet(
            &self.state,
            user_id,
            &mut self.messenger_buddy_presence,
        )
        .await?;
        self.send(&format!("@M{}", updates))?;
        Ok(())
    }

    async fn messenger_follow_buddy(&self, packet: &str) -> Result<()> {
        let buddy_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let friend_ids = user_manager::get_user_friend_ids(&self.state, self.user_id()?).await;
        let error_id;
        if friend_ids.contains(&buddy_id) {
            if let Some(user) = user_manager::get_user(&self.state, buddy_id).await {
                if user.room_id > 0 {
                    if user.room_is_public {
                        self.send(&format!("D^I{}", encode_vl64(user.room_id as i32)))?;
                    } else {
                        self.send(&format!("D^H{}", encode_vl64(user.room_id as i32)))?;
                    }
                    return Ok(());
                }
                error_id = 2;
            } else {
                error_id = 1;
            }
        } else {
            error_id = 0;
        }

        if error_id != -1 {
            self.send(&format!("E]{}", encode_vl64(error_id)))?;
        }
        Ok(())
    }

    async fn guide_set_available(&self, available: bool) -> Result<()> {
        self.state
            .db
            .run_query(&format!(
                "UPDATE users SET guideavailable = '{}' WHERE id = '{}' LIMIT 1",
                if available { 1 } else { 0 },
                self.user_id()?
            ))
            .await?;
        Ok(())
    }

    async fn group_details(&self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let group_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if group_id <= 0
            || !self
                .state
                .db
                .check_exists(&format!(
                    "SELECT id FROM groups_details WHERE id = '{}' LIMIT 1",
                    group_id
                ))
                .await
        {
            return Ok(());
        }

        let details = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT name,description,roomid FROM groups_details WHERE id = '{}' LIMIT 1",
                group_id
            ))
            .await?;
        if details.len() < 3 {
            return Ok(());
        }

        let name = details[0].clone();
        let description = details[1].clone();
        let mut room_id = details[2].parse::<i64>().unwrap_or(0);
        let room_name = if room_id > 0 {
            self.state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT name FROM rooms WHERE id = '{}' LIMIT 1",
                    room_id
                ))
                .await
        } else {
            room_id = -1;
            String::new()
        };

        self.send(&format!(
            "Dw{}{}\u{2}{}\u{2}{}{}\u{2}",
            encode_vl64(group_id as i32),
            name,
            description,
            encode_vl64(room_id as i32),
            room_name
        ))?;
        Ok(())
    }

    async fn cfh_minimum_rank(&self) -> u8 {
        self.state
            .db
            .run_read_unsafe_i64(
                "SELECT minrank FROM system_fuserights WHERE fuseright = 'fuse_receive_calls_for_help' LIMIT 1",
            )
            .await
            .clamp(1, i64::from(u8::MAX)) as u8
    }

    async fn call_for_help_status(&self) -> Result<()> {
        let username = self.username.clone().unwrap_or_default();
        let row = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT id,date,message,picked_up FROM cms_help WHERE username = '{}' AND picked_up = '0' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await
            .unwrap_or_default();
        if row.len() < 4 {
            self.send("D\u{7f}H")?;
            return Ok(());
        }

        self.send(&format!(
            "D\u{7f}I{}\u{2}{}\u{2}{}\u{2}",
            row[0], row[1], row[2]
        ))?;
        Ok(())
    }

    async fn call_for_help_delete_own(&self) -> Result<()> {
        let username = self.username.clone().unwrap_or_default();
        let cfh_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT id FROM cms_help WHERE username = '{}' AND picked_up = '0' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await;
        self.state
            .db
            .run_query(&format!(
                "DELETE FROM cms_help WHERE picked_up = '0' AND username = '{}' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await?;
        self.send("D\u{7f}H")?;

        let payload = format!(
            "BT{}\u{2}IUser Deleted!\u{2}User Deleted!\u{2}User Deleted!\u{2}{}\u{2}\u{2}H\u{2}{}",
            encode_vl64(cfh_id as i32),
            encode_vl64(0),
            encode_vl64(0)
        );
        user_manager::send_to_rank(&self.state, self.cfh_minimum_rank().await, true, &payload)
            .await;
        Ok(())
    }

    async fn call_for_help_send(&self, packet: &str) -> Result<()> {
        let username = self.username.clone().unwrap_or_default();
        if self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM cms_help WHERE username = '{}' AND picked_up = '0' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await
        {
            return Ok(());
        }

        let message_length = match packet.get(2..4) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        if message_length == 0 {
            return Ok(());
        }
        let Some(cfh_message) = packet.get(4..4 + message_length) else {
            return Ok(());
        };

        self.state
            .db
            .run_query(&format!(
                "INSERT INTO cms_help (username,ip,message,date,picked_up,subject,roomid) VALUES ('{}','{}','{}','{}','0','CFH message [hotel]','{}')",
                Database::stripslash(&username),
                Database::stripslash(&self.remote_ip),
                Database::stripslash(cfh_message),
                Database::stripslash(&chrono::Local::now().to_string()),
                self.current_room_id
            ))
            .await?;
        let cfh_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT id FROM cms_help WHERE username = '{}' AND picked_up = '0' LIMIT 1",
                Database::stripslash(&username)
            ))
            .await;
        let room_name = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM rooms WHERE id = '{}' LIMIT 1",
                self.current_room_id
            ))
            .await;

        self.send("EAH")?;
        let payload = format!(
            "BT{}\u{2}ISent: {}\u{2}{}\u{2}{}\u{2}{}\u{2}{}\u{2}I\u{2}{}",
            encode_vl64(cfh_id as i32),
            chrono::Local::now(),
            username,
            cfh_message,
            encode_vl64(self.current_room_id as i32),
            room_name,
            encode_vl64(self.current_room_id as i32)
        );
        user_manager::send_to_rank(&self.state, self.cfh_minimum_rank().await, true, &payload)
            .await;
        Ok(())
    }

    async fn call_for_help_reply(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_receive_calls_for_help")
            .await
        {
            return Ok(());
        }

        let id_length = match packet.get(2..4) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        let cfh_id = match packet.get(4..4 + id_length) {
            Some(raw) => match decode_vl64(raw) {
                Ok((value, _)) => value as i64,
                Err(_) => return Ok(()),
            },
            None => return Ok(()),
        };
        let cfh_reply = packet
            .get(id_length + 6..)
            .or_else(|| packet.get(id_length + 4..))
            .unwrap_or_default();

        let to_username = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT username FROM cms_help WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await;
        if to_username.is_empty() {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "cfh_fail").await?
            ))?;
            return Ok(());
        }

        let to_user_id = user_manager::get_user_id(&self.state, &to_username).await;
        if let Some(to_user) = user_manager::get_user(&self.state, to_user_id).await {
            let _ = to_user.sender.send(format!("DR{}\u{2}", cfh_reply));
            self.state
                .db
                .run_query(&format!(
                    "UPDATE cms_help SET picked_up = '{}' WHERE id = '{}' LIMIT 1",
                    Database::stripslash(&self.username.clone().unwrap_or_default()),
                    cfh_id
                ))
                .await?;
        }
        Ok(())
    }

    async fn call_for_help_delete_by_staff(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_receive_calls_for_help")
            .await
        {
            return Ok(());
        }

        let id_length = match packet.get(2..4) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        let cfh_id = match packet.get(4..4 + id_length) {
            Some(raw) => match decode_vl64(raw) {
                Ok((value, _)) => value as i64,
                Err(_) => return Ok(()),
            },
            None => return Ok(()),
        };
        let row = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT username,message,date,picked_up,roomid FROM cms_help WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await
            .unwrap_or_default();
        if row.len() < 5 {
            return Ok(());
        }
        if row[3] == "1" {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "cfh_picked_up").await?
            ))?;
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "DELETE FROM cms_help WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await?;
        let payload = format!(
            "BT{}\u{2}HStaff Deleted!\u{2}Staff Deleted!\u{2}Staff Deleted!\u{2}{}\u{2}\u{2}H\u{2}{}",
            encode_vl64(cfh_id as i32),
            encode_vl64(0),
            encode_vl64(0)
        );
        user_manager::send_to_rank(&self.state, self.cfh_minimum_rank().await, true, &payload)
            .await;
        Ok(())
    }

    async fn call_for_help_pickup(&self, packet: &str) -> Result<()> {
        let cfh_id = match decode_vl64(packet.get(4..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        if !self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM cms_help WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await
        {
            self.send(&string_manager::get_string(&self.state, "cfh_deleted").await?)?;
            return Ok(());
        }

        let row = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT picked_up,username,message,roomid FROM cms_help WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await
            .unwrap_or_default();
        if row.len() < 4 {
            return Ok(());
        }

        let room_name = self
            .state
            .db
            .run_read_unsafe_string(&format!(
                "SELECT name FROM rooms WHERE id = '{}' LIMIT 1",
                row[3].parse::<i64>().unwrap_or(0)
            ))
            .await;
        if row[0] == "1" {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "cfh_picked_up").await?
            ))?;
        } else {
            let room_id = row[3].parse::<i32>().unwrap_or(0);
            let payload = format!(
                "BT{}\u{2}IPicked up: {}\u{2}{}\u{2}{}\u{2}{}\u{2}{}\u{2}I\u{2}{}",
                encode_vl64(cfh_id as i32),
                chrono::Local::now(),
                row[1],
                row[2],
                encode_vl64(room_id),
                room_name,
                encode_vl64(room_id)
            );
            user_manager::send_to_rank(&self.state, self.cfh_minimum_rank().await, true, &payload)
                .await;
        }

        self.state
            .db
            .run_query(&format!(
                "UPDATE cms_help SET picked_up = '1' WHERE id = '{}' LIMIT 1",
                cfh_id
            ))
            .await?;
        Ok(())
    }

    async fn modtool_action(&mut self, packet: &str) -> Result<()> {
        let Some(action) = packet.get(2..4) else {
            return Ok(());
        };
        match action {
            "HH" => self.modtool_alert_user(packet).await?,
            "HI" => self.modtool_kick_user(packet).await?,
            "HJ" => self.modtool_ban_user(packet).await?,
            "IH" => self.modtool_room_alert(packet).await?,
            "II" => self.modtool_room_kick(packet).await?,
            _ => {}
        }
        Ok(())
    }

    async fn modtool_alert_user(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_alert").await {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "modtool_accesserror").await?
            ))?;
            return Ok(());
        }

        let Some((message, staff_note, target_user)) = self.parse_modtool_message_target(packet)
        else {
            return Ok(());
        };
        if let Some(target) = user_manager::get_user(
            &self.state,
            user_manager::get_user_id(&self.state, &target_user).await,
        )
        .await
        {
            let _ = target.sender.send(format!("B!{}\u{2}", message));
            staff_manager::add_staff_message(
                &self.state,
                "alert",
                self.user_id()?,
                target.user_id,
                &message,
                &staff_note,
            )
            .await?;
        } else {
            self.send(&format!(
                "BK{}\r{}",
                string_manager::get_string(&self.state, "modtool_actionfail").await?,
                string_manager::get_string(&self.state, "modtool_usernotfound").await?
            ))?;
        }
        Ok(())
    }

    async fn modtool_kick_user(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_kick").await {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "modtool_accesserror").await?
            ))?;
            return Ok(());
        }

        let Some((message, staff_note, target_user)) = self.parse_modtool_message_target(packet)
        else {
            return Ok(());
        };
        let target_id = user_manager::get_user_id(&self.state, &target_user).await;
        if target_id <= 0 {
            self.send(&format!(
                "BK{}\r{}",
                string_manager::get_string(&self.state, "modtool_actionfail").await?,
                string_manager::get_string(&self.state, "modtool_usernotfound").await?
            ))?;
            return Ok(());
        }
        let target_rank = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT rank FROM users WHERE id = '{}' LIMIT 1",
                target_id
            ))
            .await as u8;
        if target_rank >= self.rank {
            self.send(&format!(
                "BK{}\r{}",
                string_manager::get_string(&self.state, "modtool_actionfail").await?,
                string_manager::get_string(&self.state, "modtool_rankerror").await?
            ))?;
            return Ok(());
        }

        self.kick_online_user_from_current_room(target_id, &message)
            .await?;
        staff_manager::add_staff_message(
            &self.state,
            "kick",
            self.user_id()?,
            target_id,
            &message,
            &staff_note,
        )
        .await?;
        Ok(())
    }

    async fn modtool_ban_user(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_ban").await {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "modtool_accesserror").await?
            ))?;
            return Ok(());
        }

        let Some((message, staff_note, target_user, ban_hours, ban_ip)) =
            self.parse_modtool_ban_target(packet)
        else {
            return Ok(());
        };
        if ban_hours == 0 {
            return Ok(());
        }

        let user_details = self
            .state
            .db
            .run_read_row(&format!(
                "SELECT id,rank,ipaddress_last FROM users WHERE name = '{}' LIMIT 1",
                Database::stripslash(&target_user)
            ))
            .await
            .unwrap_or_default();
        if user_details.len() < 3 {
            self.send(&format!(
                "BK{}\r{}",
                string_manager::get_string(&self.state, "modtool_actionfail").await?,
                string_manager::get_string(&self.state, "modtool_usernotfound").await?
            ))?;
            return Ok(());
        }
        let target_id = user_details[0].parse::<i64>().unwrap_or(0);
        let target_rank = user_details[1].parse::<u8>().unwrap_or(1);
        if target_rank >= self.rank {
            self.send(&format!(
                "BK{}\r{}",
                string_manager::get_string(&self.state, "modtool_actionfail").await?,
                string_manager::get_string(&self.state, "modtool_rankerror").await?
            ))?;
            return Ok(());
        }

        staff_manager::add_staff_message(
            &self.state,
            "ban",
            self.user_id()?,
            target_id,
            &message,
            &staff_note,
        )
        .await?;
        let report = if ban_ip
            && rank_manager::contains_right(&self.state, self.rank, "fuse_superban").await
        {
            user_manager::set_ban_ip(&self.state, &user_details[2], ban_hours as i64, &message)
                .await;
            user_manager::generate_ban_report_for_ip(&self.state, &user_details[2]).await
        } else {
            user_manager::set_ban_user(&self.state, target_id, ban_hours as i64, &message).await;
            user_manager::generate_ban_report_for_user(&self.state, target_id).await
        };
        self.send(&format!("BK{}", report))?;
        Ok(())
    }

    async fn modtool_room_alert(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_alert").await {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "modtool_accesserror").await?
            ))?;
            return Ok(());
        }
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let Some((message, staff_note)) = self.parse_modtool_room_message(packet) else {
            return Ok(());
        };
        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        self.broadcast_room_users(&room, &format!("B!{}\u{2}", message), None)
            .await;
        staff_manager::add_staff_message(
            &self.state,
            "ralert",
            self.user_id()?,
            self.current_room_id,
            &message,
            &staff_note,
        )
        .await?;
        Ok(())
    }

    async fn modtool_room_kick(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_kick").await {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "modtool_accesserror").await?
            ))?;
            return Ok(());
        }
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let Some((message, staff_note)) = self.parse_modtool_room_message(packet) else {
            return Ok(());
        };
        self.room_kick_lower_rank_users(&message).await?;
        staff_manager::add_staff_message(
            &self.state,
            "rkick",
            self.user_id()?,
            self.current_room_id,
            &message,
            &staff_note,
        )
        .await?;
        Ok(())
    }

    fn parse_modtool_message_target(&self, packet: &str) -> Option<(String, String, String)> {
        let message_len = decode_b64(packet.get(4..6)?).ok()? as usize;
        let message = packet.get(6..6 + message_len)?.replace('\u{1}', " ");
        let staff_note_len =
            decode_b64(packet.get(6 + message_len..8 + message_len)?).ok()? as usize;
        let staff_note = packet
            .get(8 + message_len..8 + message_len + staff_note_len)?
            .to_string();
        let target_user = packet.get(10 + message_len + staff_note_len..)?.to_string();
        if message.is_empty() || target_user.is_empty() {
            return None;
        }
        Some((message, staff_note, target_user))
    }

    fn parse_modtool_room_message(&self, packet: &str) -> Option<(String, String)> {
        let message_len = decode_b64(packet.get(4..6)?).ok()? as usize;
        let message = packet.get(6..6 + message_len)?.replace('\u{1}', " ");
        let staff_note_len =
            decode_b64(packet.get(6 + message_len..8 + message_len)?).ok()? as usize;
        let staff_note = packet
            .get(8 + message_len..8 + message_len + staff_note_len)?
            .to_string();
        if message.is_empty() {
            return None;
        }
        Some((message, staff_note))
    }

    fn parse_modtool_ban_target(
        &self,
        packet: &str,
    ) -> Option<(String, String, String, i32, bool)> {
        let message_len = decode_b64(packet.get(4..6)?).ok()? as usize;
        let message = packet.get(6..6 + message_len)?.replace('\u{1}', " ");
        let staff_note_len =
            decode_b64(packet.get(6 + message_len..8 + message_len)?).ok()? as usize;
        let staff_note = packet
            .get(8 + message_len..8 + message_len + staff_note_len)?
            .to_string();
        let target_user_len = decode_b64(
            packet.get(8 + message_len + staff_note_len..10 + message_len + staff_note_len)?,
        )
        .ok()? as usize;
        let target_user = packet
            .get(
                10 + message_len + staff_note_len
                    ..10 + message_len + staff_note_len + target_user_len,
            )?
            .to_string();
        let (ban_hours, _) =
            decode_vl64(packet.get(10 + message_len + staff_note_len + target_user_len..)?).ok()?;
        Some((
            message,
            staff_note,
            target_user,
            ban_hours,
            packet.ends_with('I'),
        ))
    }

    async fn room_give_rights(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.is_owner {
            return Ok(());
        }

        let target_name = packet.get(2..).unwrap_or_default();
        let target_id = user_manager::get_user_id(&self.state, target_name).await;
        let Some(target_online) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if !target_online.in_room || target_online.room_id != self.current_room_id {
            return Ok(());
        }

        let target_is_owner = self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                self.current_room_id,
                Database::stripslash(target_name)
            ))
            .await;
        let already_has_rights = self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM room_rights WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                self.current_room_id, target_id
            ))
            .await;
        if target_is_owner || already_has_rights {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "INSERT INTO room_rights(roomid,userid) VALUES ('{}','{}')",
                self.current_room_id, target_id
            ))
            .await?;

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if let Some(index) = room
            .users
            .iter()
            .position(|entry| entry.user_id == target_id)
        {
            room.users[index]
                .status_manager
                .add_status("flatctrl", "onlyfurniture");
            let status_packet = room.user_status_packet(target_id);
            room_manager::save_room(&self.state, room.clone()).await;
            if let Some(packet) = status_packet {
                self.broadcast_room_users(&room, &packet, None).await;
            }
        }
        let _ = target_online.sender.send("@j".to_string());
        Ok(())
    }

    async fn room_take_rights(&mut self, packet: &str) -> Result<()> {
        self.sync_current_room_permissions().await;
        if self.current_room_id <= 0 || self.current_room_is_public || !self.is_owner {
            return Ok(());
        }

        let target_name = packet.get(2..).unwrap_or_default();
        let target_id = user_manager::get_user_id(&self.state, target_name).await;
        let Some(target_online) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if !target_online.in_room || target_online.room_id != self.current_room_id {
            return Ok(());
        }

        let target_is_owner = self
            .state
            .db
            .check_exists(&format!(
                "SELECT id FROM rooms WHERE id = '{}' AND owner = '{}' LIMIT 1",
                self.current_room_id,
                Database::stripslash(target_name)
            ))
            .await;
        let has_rights = self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM room_rights WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                self.current_room_id, target_id
            ))
            .await;
        if target_is_owner || !has_rights {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "DELETE FROM room_rights WHERE roomid = '{}' AND userid = '{}' LIMIT 1",
                self.current_room_id, target_id
            ))
            .await?;

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        if let Some(index) = room
            .users
            .iter()
            .position(|entry| entry.user_id == target_id)
        {
            room.users[index].status_manager.remove_status("flatctrl");
            let status_packet = room.user_status_packet(target_id);
            room_manager::save_room(&self.state, room.clone()).await;
            if let Some(packet) = status_packet {
                self.broadcast_room_users(&room, &packet, None).await;
            }
        }
        let _ = target_online.sender.send("@k".to_string());
        Ok(())
    }

    async fn room_answer_doorbell(&mut self, packet: &str) -> Result<()> {
        if !self.has_rights
            && rank_manager::contains_right(&self.state, self.rank, "fuse_enter_locked_rooms").await
        {
            return Ok(());
        }

        let ringer_len = match packet.get(2..4) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        let Some(ringer) = packet.get(4..4 + ringer_len) else {
            return Ok(());
        };
        let let_in = packet.ends_with('A');

        let ringer_id = user_manager::get_user_id(&self.state, ringer).await;
        if ringer_id <= 0 {
            return Ok(());
        }
        let Some(ringer_data) = user_manager::get_user(&self.state, ringer_id).await else {
            return Ok(());
        };
        if ringer_data.room_id != self.current_room_id {
            return Ok(());
        }

        if let_in {
            self.state
                .allow_doorbell_access(ringer_id, self.current_room_id)
                .await;
            self.send_doorbell_to_room_rights(self.current_room_id, &format!("@i{}\u{2}", ringer))
                .await;
            let _ = ringer_data.sender.send("@i".to_string());
        } else {
            self.state.clear_doorbell_access(ringer_id).await;
            self.state
                .mark_doorbell_denied(ringer_id, self.current_room_id)
                .await;
            let _ = ringer_data.sender.send("BC".to_string());
        }
        Ok(())
    }

    async fn room_kick_user(&self, packet: &str) -> Result<()> {
        self.room_kick_or_ban(packet, false).await
    }

    async fn room_kick_and_ban(&self, packet: &str) -> Result<()> {
        self.room_kick_or_ban(packet, true).await
    }

    async fn room_kick_or_ban(&self, packet: &str, apply_ban: bool) -> Result<()> {
        if !self.has_rights
            || self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let target_name = packet.get(2..).unwrap_or_default();
        let target_id = user_manager::get_user_id(&self.state, target_name).await;
        let Some(target) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if !target.in_room || target.room_id != self.current_room_id {
            return Ok(());
        }

        let target_rank = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT rank FROM users WHERE id = '{}' LIMIT 1",
                target_id
            ))
            .await as u8;
        let room_owner_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT id FROM users WHERE name = (SELECT owner FROM rooms WHERE id = '{}' LIMIT 1) LIMIT 1",
                self.current_room_id
            ))
            .await;
        let target_is_owner = room_owner_id == target_id;
        if target_is_owner
            && (target_rank > self.rank
                || rank_manager::contains_right(
                    &self.state,
                    target_rank,
                    "fuse_any_room_controller",
                )
                .await)
        {
            return Ok(());
        }

        if apply_ban {
            let expire = chrono::Local::now()
                + chrono::Duration::minutes(self.config_int("rooms_roomban_duration", 30).await);
            self.state
                .db
                .run_query(&format!(
                    "INSERT INTO room_bans (roomid,userid,ban_expire) VALUES ('{}','{}','{}')",
                    self.current_room_id,
                    target_id,
                    expire.format("%Y-%m-%d %H:%M:%S")
                ))
                .await?;
        }

        self.kick_online_user_from_current_room(target_id, "").await
    }

    async fn ignore_user(&self, packet: &str) -> Result<()> {
        let username = Database::stripslash(packet.get(4..).unwrap_or_default());
        if username.is_empty() {
            return Ok(());
        }

        let target_id = user_manager::get_user_id(&self.state, &username).await;
        if target_id <= 0 {
            return Ok(());
        }
        let Some(target) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if target.rank > 3 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        self.state
            .db
            .run_query(&format!(
                "INSERT INTO user_ignores(userid,targetid) VALUES ('{}','{}')",
                user_id, target_id
            ))
            .await?;
        self.send("FcI")?;
        Ok(())
    }

    async fn unignore_user(&self, packet: &str) -> Result<()> {
        let username = Database::stripslash(packet.get(4..).unwrap_or_default());
        if username.is_empty() {
            return Ok(());
        }

        let target_id = user_manager::get_user_id(&self.state, &username).await;
        if target_id <= 0 {
            return Ok(());
        }
        let Some(target) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if target.rank > 3 {
            return Ok(());
        }

        self.state
            .db
            .run_query(&format!(
                "DELETE FROM user_ignores WHERE userid = '{}' AND targetid = '{}'",
                self.user_id()?,
                target_id
            ))
            .await?;
        self.send("FcK")?;
        Ok(())
    }

    async fn call_for_help_go_to_room(&self, packet: &str) -> Result<()> {
        if !rank_manager::contains_right(&self.state, self.rank, "fuse_receive_calls_for_help")
            .await
        {
            return Ok(());
        }

        let id_length = match packet.get(2..4) {
            Some(raw) => decode_b64(raw).unwrap_or(0) as usize,
            None => 0,
        };
        if id_length == 0 {
            return Ok(());
        }

        let cfh_id = match packet.get(4..4 + id_length) {
            Some(raw) => match decode_vl64(raw) {
                Ok((value, _)) => value as i64,
                Err(_) => return Ok(()),
            },
            None => return Ok(()),
        };
        let room_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT roomid FROM cms_help WHERE id = '{}'",
                cfh_id
            ))
            .await;
        if room_id <= 0 {
            return Ok(());
        }

        let is_public_room = if let Some(room) = room_manager::get_room(&self.state, room_id).await
        {
            room.is_publicroom
        } else {
            self.state
                .db
                .check_exists(&format!(
                    "SELECT id FROM rooms WHERE id = '{}' AND owner IS NULL LIMIT 1",
                    room_id
                ))
                .await
        };

        if is_public_room {
            self.send(&format!("D^I{}", encode_vl64(room_id as i32)))?;
        } else {
            self.send(&format!("D^H{}", encode_vl64(room_id as i32)))?;
        }
        Ok(())
    }

    async fn messenger_invite_buddies(&self, packet: &str) -> Result<()> {
        if self.current_room_uid.is_none() {
            return Ok(());
        }

        let args = packet.get(2..).unwrap_or_default();
        let (amount, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let mut remaining = &args[used..];
        let friend_ids = user_manager::get_user_friend_ids(&self.state, self.user_id()?).await;
        let mut ids = Vec::new();

        for _ in 0..amount {
            if remaining.is_empty() {
                return Ok(());
            }
            let (id, used) = match decode_vl64(remaining) {
                Ok(result) => result,
                Err(_) => return Ok(()),
            };
            let id = id as i64;
            if friend_ids.contains(&id) && user_manager::contains_user_by_id(&self.state, id).await
            {
                ids.push(id);
            }
            remaining = &remaining[used..];
        }

        let message = remaining.get(2..).unwrap_or_default();
        let data = format!("BG{}{}\u{2}", encode_vl64(self.user_id()? as i32), message);
        for id in ids {
            if let Some(user) = user_manager::get_user(&self.state, id).await {
                let _ = user.sender.send(data.clone());
            }
        }
        Ok(())
    }

    async fn console_search_setup(&self) -> Result<()> {
        self.send("HRL")?;
        Ok(())
    }

    async fn console_search(&self, packet: &str) -> Result<()> {
        let search = Database::stripslash(packet.get(4..).unwrap_or_default());
        let mut packet_out = "Fs".to_string();
        let mut packet_friends = String::new();
        let mut packet_others = String::new();
        let mut count_friends = 0_i32;
        let mut count_others = 0_i32;
        let friend_ids = user_manager::get_user_friend_ids(&self.state, self.user_id()?).await;

        let ids = self
            .state
            .db
            .run_read_column_i64(&format!(
                "SELECT id FROM users WHERE name LIKE '%{}%' LIMIT 50",
                search
            ))
            .await
            .unwrap_or_default();

        for this_id in ids {
            let online = user_manager::contains_user_by_id(&self.state, this_id).await;
            let online_str = if online { "I" } else { "H" };
            let row = self
                .state
                .db
                .run_read_row(&format!(
                    "SELECT name, mission, lastvisit, figure FROM users WHERE id = {} LIMIT 1",
                    this_id
                ))
                .await?;
            if row.len() < 4 {
                continue;
            }

            let packet_add = format!(
                "{}{}\u{2}{}\u{2}{}{}\u{2}{}{}\u{2}{}\u{2}",
                encode_vl64(this_id as i32),
                row[0],
                row[1],
                online_str,
                online_str,
                online_str,
                if online { row[3].as_str() } else { "" },
                if online { "" } else { row[2].as_str() },
            );

            if friend_ids.contains(&this_id) {
                count_friends += 1;
                packet_friends.push_str(&packet_add);
            } else {
                count_others += 1;
                packet_others.push_str(&packet_add);
            }
        }

        packet_out.push_str(&encode_vl64(count_friends));
        packet_out.push_str(&packet_friends);
        packet_out.push_str(&encode_vl64(count_others));
        packet_out.push_str(&packet_others);
        self.send(&packet_out)?;
        Ok(())
    }

    async fn enter_room_loading_advertisement(&self) -> Result<()> {
        // The original Holograph flow hard-disabled loading ads at room entry.
        self.send("DB0")?;
        Ok(())
    }

    async fn enter_room_via_teleporter(&self) -> Result<()> {
        self.send("@S")?;
        Ok(())
    }

    async fn room_enter_teleporter(&mut self, packet: &str) -> Result<()> {
        if self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = packet
            .get(2..)
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .unwrap_or(0);
        if item_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let mut room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        let Some(teleporter) = room.floor_item(item_id).cloned() else {
            return Ok(());
        };
        let teleporter_sprite = teleporter.sprite(&self.state).await;
        if !is_legacy_teleporter_sprite(&teleporter_sprite) {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        };

        let room_user = &room.users[index];
        if !is_user_at_teleporter_entrance(
            room_user.x,
            room_user.y,
            teleporter.x,
            teleporter.y,
            teleporter.z,
        ) {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }

        room.users[index].goal_x = -1;
        room.users[index].goal_y = -1;
        if !room.queue_user_single_step(user_id, teleporter.x, teleporter.y, true) {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }
        room_manager::save_room(&self.state, room).await;
        Ok(())
    }

    async fn room_use_teleporter(&mut self, packet: &str) -> Result<()> {
        if self.current_room_is_public
            || self.current_room_id <= 0
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = packet
            .get(2..)
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .unwrap_or(0);
        if item_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let username = self.username.clone().unwrap_or_default();
        let mut room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        let Some(teleporter_1) = room.floor_item(item_id).cloned() else {
            return Ok(());
        };
        let sprite = teleporter_1.sprite(&self.state).await;
        if !is_legacy_teleporter_sprite(&sprite) {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        };

        let room_user = &room.users[index];
        if !is_user_on_teleporter(room_user.x, room_user.y, teleporter_1.x, teleporter_1.y) {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }

        let teleporter_2_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT teleportid FROM furniture WHERE id = '{}' LIMIT 1",
                item_id
            ))
            .await;
        let teleporter_2_room_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT roomid FROM furniture WHERE id = '{}' LIMIT 1",
                teleporter_2_id
            ))
            .await;
        if teleporter_2_room_id <= 0 {
            room_manager::save_room(&self.state, room).await;
            return Ok(());
        }

        if teleporter_2_room_id == self.current_room_id {
            room.users[index].walk_lock = true;
            room_manager::save_room(&self.state, room).await;
            tokio::spawn(run_same_room_teleporter_use(
                Arc::new(self.state.as_ref().clone()),
                self.current_room_id,
                user_id,
                teleporter_1.id,
                teleporter_2_id,
                username,
                sprite,
            ));
        } else {
            room.users[index].walk_lock = true;
            self.pending_teleporter_id = teleporter_2_id;
            self.pending_teleporter_room_id = teleporter_2_room_id;
            self.send(&format!(
                "@~{}{}",
                encode_vl64(teleporter_2_id as i32),
                encode_vl64(teleporter_2_room_id as i32)
            ))?;
            self.broadcast_room_users(
                &room,
                &format!("AY{}/{}/{}", teleporter_1.id, username, sprite),
                None,
            )
            .await;
            room_manager::save_room(&self.state, room).await;
        }

        Ok(())
    }

    async fn user_tags(&self, packet: &str) -> Result<()> {
        let owner_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let tags = self
            .state
            .db
            .run_read_column_string(&format!(
                "SELECT tag FROM cms_tags WHERE ownerid = '{}' LIMIT 20",
                owner_id
            ))
            .await
            .unwrap_or_default();
        let mut list = format!(
            "{}{}",
            encode_vl64(owner_id as i32),
            encode_vl64(tags.len() as i32)
        );
        for tag in tags {
            list.push_str(&tag);
            list.push('\u{2}');
        }
        self.send(&format!("E^{}", list))?;
        Ok(())
    }

    async fn execute_speech_command(&mut self, text: &str) -> Result<bool> {
        let args = text.split_whitespace().collect::<Vec<_>>();
        let Some(command) = args.first().copied() else {
            return Ok(false);
        };

        match command {
            "emptyhand" => {
                let user_id = self.user_id()?;
                self.state
                    .db
                    .run_query(&format!(
                        "DELETE FROM furniture WHERE ownerid = '{}' AND roomid = '0'",
                        user_id
                    ))
                    .await?;
                self.refresh_hand("new").await?;
                Ok(true)
            }
            "alert" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_alert").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }
                let message = args.iter().skip(2).copied().collect::<Vec<_>>().join(" ");
                if message.is_empty() {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                if let Some(target) = user_manager::get_user(&self.state, target_id).await {
                    let _ = target.sender.send(format!("B!{}\u{2}", message));
                    staff_manager::add_staff_message(
                        &self.state,
                        "alert",
                        self.user_id()?,
                        target_id,
                        &message,
                        "",
                    )
                    .await?;
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_success").await?
                    ))?;
                } else {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                }
                Ok(true)
            }
            "roomalert" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_alert").await {
                    return Ok(false);
                }
                if self.current_room_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let message = text.get(10..).unwrap_or_default().to_string();
                let room = room_manager::load_room(
                    &self.state,
                    self.current_room_id,
                    self.current_room_is_public,
                )
                .await?;
                self.broadcast_room_users(&room, &format!("B!{}\u{2}", message), None)
                    .await;
                staff_manager::add_staff_message(
                    &self.state,
                    "ralert",
                    self.user_id()?,
                    self.current_room_id,
                    &message,
                    "",
                )
                .await?;
                Ok(true)
            }
            "kick" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_kick").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let target_rank = self
                    .state
                    .db
                    .run_read_unsafe_i64(&format!(
                        "SELECT rank FROM users WHERE id = '{}' LIMIT 1",
                        target_id
                    ))
                    .await as u8;
                if target_rank >= self.rank {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let message = args.iter().skip(2).copied().collect::<Vec<_>>().join(" ");
                if self.kick_user_from_room(target_id, &message).await? {
                    staff_manager::add_staff_message(
                        &self.state,
                        "kick",
                        self.user_id()?,
                        target_id,
                        &message,
                        "",
                    )
                    .await?;
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_success").await?
                    ))?;
                } else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                }
                Ok(true)
            }
            "roomkick" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_kick").await {
                    return Ok(false);
                }
                if self.current_room_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let message = args.iter().skip(1).copied().collect::<Vec<_>>().join(" ");
                self.room_kick_lower_rank_users(&message).await?;
                staff_manager::add_staff_message(
                    &self.state,
                    "rkick",
                    self.user_id()?,
                    self.current_room_id,
                    &message,
                    "",
                )
                .await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_success").await?
                ))?;
                Ok(true)
            }
            "shutup" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_mute").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let target_rank = self
                    .state
                    .db
                    .run_read_unsafe_i64(&format!(
                        "SELECT rank FROM users WHERE id = '{}' LIMIT 1",
                        target_id
                    ))
                    .await as u8;
                if target_rank >= self.rank || self.user_muted_state(target_id).await == Some(true)
                {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let message = args.iter().skip(2).copied().collect::<Vec<_>>().join(" ");
                self.set_user_muted(target_id, true).await;
                if let Some(target) = user_manager::get_user(&self.state, target_id).await {
                    let muted = string_manager::get_string(&self.state, "scommand_muted")
                        .await
                        .unwrap_or_else(|_| "scommand_muted".to_string());
                    let _ = target.sender.send(format!("BK{}\r{}", muted, message));
                }
                staff_manager::add_staff_message(
                    &self.state,
                    "mute",
                    self.user_id()?,
                    target_id,
                    &message,
                    "",
                )
                .await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_success").await?
                ))?;
                Ok(true)
            }
            "unmute" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_mute").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let target_rank = self
                    .state
                    .db
                    .run_read_unsafe_i64(&format!(
                        "SELECT rank FROM users WHERE id = '{}' LIMIT 1",
                        target_id
                    ))
                    .await as u8;
                if target_rank >= self.rank || self.user_muted_state(target_id).await != Some(true)
                {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                self.set_user_muted(target_id, false).await;
                if let Some(target) = user_manager::get_user(&self.state, target_id).await {
                    let unmuted = string_manager::get_string(&self.state, "scommand_unmuted")
                        .await
                        .unwrap_or_else(|_| "scommand_unmuted".to_string());
                    let _ = target.sender.send(format!("BK{}", unmuted));
                }
                staff_manager::add_staff_message(
                    &self.state,
                    "unmute",
                    self.user_id()?,
                    target_id,
                    "",
                    "",
                )
                .await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_success").await?
                ))?;
                Ok(true)
            }
            "roomshutup" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_mute").await {
                    return Ok(false);
                }
                if self.current_room_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let message = args.iter().skip(1).copied().collect::<Vec<_>>().join(" ");
                self.room_set_mute_lower_rank_users(true, &message).await?;
                staff_manager::add_staff_message(
                    &self.state,
                    "rmute",
                    self.user_id()?,
                    self.current_room_id,
                    &message,
                    "",
                )
                .await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_success").await?
                ))?;
                Ok(true)
            }
            "roomunmute" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_room_mute").await {
                    return Ok(false);
                }
                if self.current_room_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                self.room_set_mute_lower_rank_users(false, "").await?;
                staff_manager::add_staff_message(
                    &self.state,
                    "runmute",
                    self.user_id()?,
                    self.current_room_id,
                    "",
                    "",
                )
                .await?;
                self.send(&format!(
                    "BK{}",
                    string_manager::get_string(&self.state, "scommand_success").await?
                ))?;
                Ok(true)
            }
            "ban" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_ban").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let ban_hours = args
                    .get(2)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0);
                let reason = args.iter().skip(3).copied().collect::<Vec<_>>().join(" ");
                let user_details = self
                    .state
                    .db
                    .run_read_row(&format!(
                        "SELECT id,rank FROM users WHERE name = '{}' LIMIT 1",
                        Database::stripslash(target_name)
                    ))
                    .await
                    .unwrap_or_default();
                if user_details.len() < 2 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }
                let target_id = user_details[0].parse::<i64>().unwrap_or(0);
                let target_rank = user_details[1].parse::<u8>().unwrap_or(0);
                if target_rank > self.rank {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_rankerror")
                            .await
                            .unwrap_or_else(|_| "modtool_rankerror".to_string())
                    ))?;
                    return Ok(true);
                }
                if ban_hours == 0 || reason.is_empty() {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                staff_manager::add_staff_message(
                    &self.state,
                    "ban",
                    self.user_id()?,
                    target_id,
                    &reason,
                    "",
                )
                .await?;
                user_manager::set_ban_user(&self.state, target_id, ban_hours, &reason).await;
                self.send(&format!(
                    "BK{}",
                    user_manager::generate_ban_report_for_user(&self.state, target_id).await
                ))?;
                Ok(true)
            }
            "superban" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_superban").await {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let ban_hours = args
                    .get(2)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0);
                let reason = args.iter().skip(3).copied().collect::<Vec<_>>().join(" ");
                let user_details = self
                    .state
                    .db
                    .run_read_row(&format!(
                        "SELECT id,rank,ipaddress_last FROM users WHERE name = '{}' LIMIT 1",
                        Database::stripslash(target_name)
                    ))
                    .await
                    .unwrap_or_default();
                if user_details.len() < 3 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }
                let target_id = user_details[0].parse::<i64>().unwrap_or(0);
                let target_rank = user_details[1].parse::<u8>().unwrap_or(0);
                if target_rank > self.rank {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_rankerror")
                            .await
                            .unwrap_or_else(|_| "modtool_rankerror".to_string())
                    ))?;
                    return Ok(true);
                }
                if ban_hours == 0 || reason.is_empty() {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let ip = user_details[2].clone();
                staff_manager::add_staff_message(
                    &self.state,
                    "ban",
                    self.user_id()?,
                    target_id,
                    &reason,
                    "",
                )
                .await?;
                user_manager::set_ban_ip(&self.state, &ip, ban_hours, &reason).await;
                self.send(&format!(
                    "BK{}",
                    user_manager::generate_ban_report_for_ip(&self.state, &ip).await
                ))?;
                Ok(true)
            }
            "info" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_moderator_access")
                    .await
                {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }
                self.send(&format!(
                    "BK{}",
                    user_manager::generate_user_info(&self.state, target_id, self.rank).await
                ))?;
                Ok(true)
            }
            "find" | "teleport" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_moderator_access")
                    .await
                {
                    return Ok(false);
                }
                if command == "find" {
                    let Some(target_name) = args.get(1).copied() else {
                        self.send(&format!(
                            "BK{}",
                            string_manager::get_string(&self.state, "scommand_failed").await?
                        ))?;
                        return Ok(true);
                    };
                    let target_id = user_manager::get_user_id(&self.state, target_name).await;
                    if target_id <= 0 {
                        self.send(&format!(
                            "BK{}\r{}",
                            string_manager::get_string(&self.state, "modtool_actionfailed")
                                .await
                                .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                            string_manager::get_string(&self.state, "modtool_usernotfound")
                                .await
                                .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                        ))?;
                        return Ok(true);
                    }
                    let Some(target) = user_manager::get_user(&self.state, target_id).await else {
                        self.send(&format!(
                            "BK{}\r{}",
                            string_manager::get_string(&self.state, "modtool_actionfailed")
                                .await
                                .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                            string_manager::get_string(&self.state, "modtool_usernotfound")
                                .await
                                .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                        ))?;
                        return Ok(true);
                    };
                    if target.in_room && target.room_id > 0 {
                        if target.room_is_public {
                            self.send(&format!("D^I{}", encode_vl64(target.room_id as i32)))?;
                        } else {
                            self.send(&format!("D^H{}", encode_vl64(target.room_id as i32)))?;
                        }
                    } else {
                        self.send(&format!(
                            "BKUnable to teleport, {} is not in a room.",
                            target.username
                        ))?;
                    }
                } else {
                    if self.current_room_id <= 0 {
                        self.send(&format!(
                            "BK{}",
                            string_manager::get_string(&self.state, "scommand_failed").await?
                        ))?;
                        return Ok(true);
                    }
                    let user_id = self.user_id()?;
                    let mut room = room_manager::load_room(
                        &self.state,
                        self.current_room_id,
                        self.current_room_is_public,
                    )
                    .await?;
                    if room.set_special_teleportable(user_id, true) {
                        self.armed_tile_teleport = true;
                        room_manager::save_room(&self.state, room).await;
                        self.send("BKClick a tile to teleport there.")?;
                    } else {
                        self.send(&format!(
                            "BK{}",
                            string_manager::get_string(&self.state, "scommand_failed").await?
                        ))?;
                    }
                }
                Ok(true)
            }
            "warp" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_moderator_access")
                    .await
                {
                    return Ok(false);
                }
                let Some(goal_x) = args.get(1).and_then(|value| value.parse::<i32>().ok()) else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let Some(goal_y) = args.get(2).and_then(|value| value.parse::<i32>().ok()) else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                if self.current_room_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let user_id = self.user_id()?;
                let mut room = room_manager::load_room(
                    &self.state,
                    self.current_room_id,
                    self.current_room_is_public,
                )
                .await?;
                self.teleport_to_room_tile(&mut room, user_id, goal_x, goal_y)
                    .await?;
                Ok(true)
            }
            "teletome" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_moderator_access")
                    .await
                {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }
                let Some(target) = user_manager::get_user(&self.state, target_id).await else {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                };
                let _ = target.sender.send(format!(
                    "BKYou are being teleported to where {} is.",
                    self.username.clone().unwrap_or_default()
                ));
                if self.current_room_is_public {
                    let _ = target
                        .sender
                        .send(format!("D^I{}", encode_vl64(self.current_room_id as i32)));
                } else {
                    let _ = target
                        .sender
                        .send(format!("D^H{}", encode_vl64(self.current_room_id as i32)));
                }
                Ok(true)
            }
            "offline" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                let Some(minutes) = args.get(1).and_then(|value| value.parse::<i32>().ok()) else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                user_manager::send_to_all(&self.state, &format!("Dc{}", encode_vl64(minutes)))
                    .await;
                staff_manager::add_staff_message(
                    &self.state,
                    "offline",
                    self.user_id()?,
                    0,
                    &format!("mm={minutes}"),
                    "",
                )
                .await?;
                Ok(true)
            }
            "position" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                let Some(current_room_uid) = self.current_room_uid else {
                    return Ok(true);
                };
                if self.current_room_id <= 0 {
                    return Ok(true);
                }
                let Some(room) = room_manager::get_room(&self.state, self.current_room_id).await
                else {
                    return Ok(true);
                };
                let Some(room_user) = room
                    .users
                    .iter()
                    .find(|entry| entry.room_uid == current_room_uid)
                else {
                    return Ok(true);
                };
                self.send(&format!(
                    "BKX: {}\rY: {}\rZ: {}\rHeight: {}",
                    room_user.x, room_user.y, room_user.z1, room_user.h
                ))?;
                Ok(true)
            }
            "ha" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                let message = text.get(3..).unwrap_or_default().to_string();
                user_manager::send_to_all(
                    &self.state,
                    &format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "scommand_hotelalert")
                            .await
                            .unwrap_or_else(|_| "scommand_hotelalert".to_string()),
                        message
                    ),
                )
                .await;
                staff_manager::add_staff_message(
                    &self.state,
                    "halert",
                    self.user_id()?,
                    0,
                    &message,
                    "",
                )
                .await?;
                Ok(true)
            }
            "ra" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_alert").await {
                    return Ok(false);
                }
                let message = text.get(3..).unwrap_or_default().to_string();
                user_manager::send_to_rank(
                    &self.state,
                    self.rank,
                    false,
                    &format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "scommand_rankalert")
                            .await
                            .unwrap_or_else(|_| "scommand_rankalert".to_string()),
                        message
                    ),
                )
                .await;
                staff_manager::add_staff_message(
                    &self.state,
                    "rankalert",
                    self.user_id()?,
                    self.rank as i64,
                    &message,
                    "",
                )
                .await?;
                Ok(true)
            }
            "refreshrooms" | "refreshhotel" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                user_manager::send_to_all(&self.state, "DBO\r").await;
                Ok(true)
            }
            "refreshcatalogue" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                self.send("HKRC")?;
                catalogue_manager::init(&self.state).await?;
                Ok(true)
            }
            "coins" => {
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    self.send("BKYou don't have the rights to give coins to someone!")?;
                    return Ok(true);
                }
                let Some(credits) = args.get(2).and_then(|value| value.parse::<i64>().ok()) else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }
                let Some(target) = user_manager::get_user(&self.state, target_id).await else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                self.state
                    .db
                    .run_query(&format!(
                        "UPDATE users SET credits = credits + {} WHERE id = '{}' LIMIT 1",
                        credits, target.user_id
                    ))
                    .await?;
                let _ = target.sender.send(format!(
                    "BKYou have recieved {} credits from a staff member!",
                    credits
                ));
                self.send(&format!(
                    "BKYou've succesfully sent {} coins to {}.",
                    credits, target.username
                ))?;
                mus_refresh_appearance(&self.state, target.user_id).await?;
                mus_refresh_valueables(&self.state, target.user_id, true, false).await?;
                Ok(true)
            }
            "givebadge" => {
                if !rank_manager::contains_right(
                    &self.state,
                    self.rank,
                    "fuse_administrator_access",
                )
                .await
                {
                    return Ok(false);
                }
                let Some(target_name) = args.get(1).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let Some(raw_badge) = args.get(2).copied() else {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                };
                let badge = Database::stripslash(raw_badge.trim());
                if badge.is_empty() {
                    self.send(&format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_failed").await?
                    ))?;
                    return Ok(true);
                }

                let target_id = user_manager::get_user_id(&self.state, target_name).await;
                if target_id <= 0 {
                    self.send(&format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "modtool_actionfailed")
                            .await
                            .unwrap_or_else(|_| "modtool_actionfailed".to_string()),
                        string_manager::get_string(&self.state, "modtool_usernotfound")
                            .await
                            .unwrap_or_else(|_| "modtool_usernotfound".to_string())
                    ))?;
                    return Ok(true);
                }

                let has_badge = self
                    .state
                    .db
                    .run_read_unsafe_i64(&format!(
                        "SELECT userid FROM users_badges WHERE userid = '{}' AND (badge = '{}' OR badgeid = '{}') LIMIT 1",
                        target_id, badge, badge
                    ))
                    .await
                    > 0;
                if !has_badge {
                    self.state
                        .db
                        .run_query(&format!(
                            "INSERT INTO users_badges(userid,badge,badgeid,iscurrent,slotid) VALUES ('{}','{}','{}','0','0')",
                            target_id, badge, badge
                        ))
                        .await?;
                }

                mus_refresh_badges(&self.state, target_id).await?;
                self.send(&format!(
                    "BKBadge {} has been given to {}.",
                    badge, target_name
                ))?;
                Ok(true)
            }
            "commands" => {
                if !rank_manager::contains_right(&self.state, self.rank, "fuse_moderator_access")
                    .await
                {
                    return Ok(false);
                }
                self.send(
                    "BK:emptyhand\r:alert <user> <message>\r:roomalert <message>\r:kick <user> <message>\r:roomkick <message>\r:shutup <user> <message>\r:unmute <user>\r:roomshutup <message>\r:roomunmute\r:ban <user> <hours> <message>\r:superban <user> <hours> <message>\r:info <username>\r:find <username>\r:teleport\r:warp X Y\r:teletome <username>\r:givebadge <user> <badge>\r:position\r:ha <message>\r:ra <message>\r:offline <minutes>\r:refreshrooms\r:refreshhotel\r:refreshcatalogue\r:coins <user> <amount>\r",
                )?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn room_typing(&mut self, is_typing: bool) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let user_id = self.user_id()?;
        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        room.users[index].is_typing = is_typing;
        let room_uid = room.users[index].room_uid;
        let recipients = room
            .users
            .iter()
            .map(|entry| entry.user_id)
            .collect::<Vec<_>>();
        room_manager::save_room(&self.state, room).await;

        let packet = format!(
            "Ei{}{}",
            encode_vl64(room_uid as i32),
            if is_typing { "I" } else { "H" }
        );
        for target_user_id in recipients {
            if let Some(user) = user_manager::get_user(&self.state, target_user_id).await {
                let _ = user.sender.send(packet.clone());
            }
        }
        Ok(())
    }

    async fn enter_room_add_user(&mut self) -> Result<()> {
        if self.current_room_id > 0 && self.consume_denied_doorbell_reset().await? {
            return Ok(());
        }
        if self.current_room_id <= 0 || !self.room_access_secondary_ok {
            return Ok(());
        }
        if self.pending_teleporter_id <= 0 && !self.room_access_primary_ok {
            return Ok(());
        }

        let Some(mut room) = room_manager::get_room(&self.state, self.current_room_id).await else {
            return Ok(());
        };
        let user_id = self.user_id()?;
        if room.users.iter().any(|entry| entry.user_id == user_id) {
            self.current_room_uid = room
                .users
                .iter()
                .find(|entry| entry.user_id == user_id)
                .map(|entry| entry.room_uid);
            return Ok(());
        }

        let room_uid = room.get_free_room_identifier();
        let mut room_user = VirtualRoomUser::new(user_id, self.current_room_id, room_uid);
        room_user.rank = self.rank;
        room_user.username = self.username.clone().unwrap_or_default();
        room_user.figure = self.figure.clone().unwrap_or_default();
        room_user.mission = self.mission.clone().unwrap_or_default();
        room_user.sex = self.sex.clone().unwrap_or_else(|| "M".to_string());
        room_user.badges = load_current_badges(&self.state, user_id).await;
        let (group_id, group_member_rank) = load_current_group_status(&self.state, user_id).await;
        room_user.group_id = group_id;
        room_user.group_member_rank = group_member_rank;
        if !room.is_publicroom {
            if self.has_rights && !self.is_owner {
                room_user
                    .status_manager
                    .add_status("flatctrl", "onlyfurniture");
            }
            if self.is_owner {
                room_user.status_manager.add_status("flatctrl", "useradmin");
            }
        }
        room_user.has_voted = self
            .state
            .db
            .check_exists(&format!(
                "SELECT userid FROM room_votes WHERE userid = '{}' AND roomid = '{}' LIMIT 1",
                user_id, self.current_room_id
            ))
            .await;
        if room.is_publicroom && room.has_swimming_pool {
            room_user.swim_outfit = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT figure_swim FROM users WHERE id = '{}' LIMIT 1",
                    user_id
                ))
                .await;
        }
        if let Some(lobby) = &room.lobby {
            room_user.game_points = self.user_game_points(lobby.is_battle_ball).await;
        }
        let teleporter_arrival_packet = if self.pending_teleporter_id > 0
            && self.pending_teleporter_room_id == self.current_room_id
        {
            let teleporter_arrival = room.floor_item(self.pending_teleporter_id).cloned();
            self.pending_teleporter_id = 0;
            self.pending_teleporter_room_id = 0;
            if let Some(teleporter) = teleporter_arrival {
                let sprite = teleporter.sprite(&self.state).await;
                room_user.x = teleporter.x;
                room_user.y = teleporter.y;
                room_user.h = teleporter.h;
                room_user.z1 = teleporter.z;
                room_user.z2 = teleporter.z;
                Some(format_teleporter_arrival_packet(
                    teleporter.id,
                    &room_user.username,
                    &sprite,
                ))
            } else {
                None
            }
        } else {
            None
        };

        self.send(&format!("@b{}", room.dynamic_statuses()))?;
        room.add_room_user(room_user.clone());
        let details_packet = room
            .user_details_packet(user_id)
            .unwrap_or_else(|| format!("@\\{}", room_user.details_string()));
        let activated_group_packet = if room.activate_group(room_user.group_id) {
            let badge = self
                .state
                .db
                .run_read_unsafe_string(&format!(
                    "SELECT badge FROM groups_details WHERE id = '{}' LIMIT 1",
                    room_user.group_id
                ))
                .await;
            if badge.is_empty() {
                None
            } else {
                Some(format!(
                    "DuI{}{}\u{2}",
                    encode_vl64(room_user.group_id as i32),
                    badge
                ))
            }
        } else {
            None
        };
        if let Some(packet) = teleporter_arrival_packet {
            self.broadcast_room_users(&room, &packet, None).await;
        }
        if let Some(lobby) = &room.lobby {
            self.broadcast_room_users(
                &room,
                &format!(
                    "CzI{}{}{}\u{2}",
                    encode_vl64(room_user.room_uid as i32),
                    room_user.game_points,
                    rank_manager::get_game_rank_title(
                        &self.state,
                        lobby.is_battle_ball,
                        room_user.game_points,
                    )
                    .await
                ),
                None,
            )
            .await;
        }
        self.broadcast_room_users(&room, &details_packet, None)
            .await;
        if let Some(packet) = activated_group_packet {
            self.broadcast_room_users(&room, &packet, None).await;
        }
        room_manager::update_room_visitor_count(
            &self.state,
            self.current_room_id,
            room.users.len() as i64,
        )
        .await?;
        room_manager::save_room(&self.state, room).await;

        self.current_room_uid = Some(room_user.room_uid);
        if let Some(mut user) = user_manager::get_user(&self.state, user_id).await {
            user.in_room = true;
            user.room_id = self.current_room_id;
            user.room_is_public = self.current_room_is_public;
            self.state.online_users.write().await.insert(user_id, user);
        }
        Ok(())
    }

    async fn leave_room(&mut self) -> Result<()> {
        self.armed_tile_teleport = false;
        if self.current_room_id <= 0 {
            if self.current_game_id.is_some() {
                // Legacy Holograph routed `@u` through leaveGame() when the session no longer had
                // an attached roomUser but was still inside game state. Preserve that fallback for
                // arena-side leaves.
                self.game_lobby_leave().await?;
            }
            return Ok(());
        }
        if self
            .state
            .active_trades
            .read()
            .await
            .contains_key(&self.user_id()?)
        {
            self.trade_abort(false).await?;
        }

        if let Some(room_uid) = self.current_room_uid {
            let mut room = room_manager::load_room(
                &self.state,
                self.current_room_id,
                self.current_room_is_public,
            )
            .await?;
            room.remove_room_user(room_uid);
            self.broadcast_room_users(&room, &format!("@]{room_uid}"), None)
                .await;
            let visitor_count = room.users.len() as i64;
            if visitor_count > 0 {
                room_manager::update_room_visitor_count(
                    &self.state,
                    self.current_room_id,
                    visitor_count,
                )
                .await?;
                room_manager::save_room(&self.state, room).await;
            } else {
                room_manager::remove_room(&self.state, self.current_room_id).await?;
            }
        }

        self.clear_room_access_markers_for_self().await?;
        self.current_room_uid = None;
        self.current_room_id = 0;
        self.current_room_is_public = false;
        self.room_access_primary_ok = false;
        self.room_access_secondary_ok = false;
        self.is_owner = false;
        self.has_rights = false;
        self.song_editor = None;
        if let Some(user_id) = self.logged_in_user_id
            && let Some(mut user) = user_manager::get_user(&self.state, user_id).await
        {
            user.in_room = false;
            user.room_id = 0;
            user.room_is_public = false;
            self.state.online_users.write().await.insert(user_id, user);
        }
        Ok(())
    }

    async fn game_lobby_checkout(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0
            || self.current_room_uid.is_none()
            || self.current_game_id.is_some()
        {
            return Ok(());
        }
        let game_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) if value >= 0 => value as i64,
            _ => return Ok(()),
        };

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(lobby) = room.lobby.as_mut() else {
            return Ok(());
        };
        let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id) else {
            return Ok(());
        };

        let player = self.build_game_player().await;
        game.add_subviewer(player);
        let payload = game.sub_payload();
        room_manager::save_room(&self.state, room).await;

        self.current_game_id = Some(game_id);
        self.current_game_room_id = Some(self.current_room_id);
        self.current_game_room_is_public = self.current_room_is_public;
        self.current_game_team_id = -1;
        self.send(&format!("Ci{}", payload))?;
        Ok(())
    }

    async fn game_lobby_request_create(&mut self) -> Result<()> {
        if self.current_room_id <= 0
            || self.current_room_uid.is_none()
            || self.current_game_id.is_some()
        {
            return Ok(());
        }
        let tickets = self.user_tickets().await;
        if tickets <= 1 {
            self.send("ClJ")?;
            return Ok(());
        }

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(lobby) = room.lobby else {
            return Ok(());
        };
        let game_points = self.user_game_points(lobby.is_battle_ball).await;
        if !lobby.valid_gamerank(game_points) {
            self.send("ClK")?;
            return Ok(());
        }

        self.send(&format!("Ck{}", lobby.get_create_game_settings()))?;
        Ok(())
    }

    async fn game_lobby_process_create(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0
            || self.current_room_uid.is_none()
            || self.current_game_id.is_some()
        {
            return Ok(());
        }
        let tickets = self.user_tickets().await;
        if tickets <= 1 {
            self.send("ClJ")?;
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(lobby) = room.lobby.as_mut() else {
            return Ok(());
        };
        let is_battle_ball = lobby.is_battle_ball;
        let game_points = self.user_game_points(is_battle_ball).await;
        if !lobby.valid_gamerank(game_points) {
            self.send("ClK")?;
            return Ok(());
        }

        let mut remaining = packet.get(2..).unwrap_or_default();
        let (key_amount, used) = match decode_vl64(remaining) {
            Ok((value, used)) if value > 0 => (value as usize, used),
            _ => return Ok(()),
        };
        remaining = &remaining[used..];

        let mut map_id = -1_i64;
        let mut team_amount = 0usize;
        let mut powerups = Vec::new();
        let mut name = String::new();

        for _ in 0..key_amount {
            if remaining.len() < 3 {
                break;
            }
            let key_len = decode_b64(&remaining[..2]).unwrap_or(0);
            if remaining.len() < key_len + 3 {
                break;
            }
            let key = remaining[2..2 + key_len].to_string();
            let value_type = remaining[2 + key_len..3 + key_len]
                .chars()
                .next()
                .unwrap_or('H');
            remaining = &remaining[3 + key_len..];

            if value_type == 'H' {
                let (value, value_used) = match decode_vl64(remaining) {
                    Ok(result) => result,
                    Err(_) => break,
                };
                match key.as_str() {
                    "fieldType" => map_id = value as i64,
                    "numTeams" => team_amount = value as usize,
                    _ => {}
                }
                remaining = &remaining[value_used..];
            } else {
                if remaining.len() < 2 {
                    break;
                }
                let value_len = decode_b64(&remaining[..2]).unwrap_or(0);
                if remaining.len() < 2 + value_len {
                    break;
                }
                let value = remaining[2..2 + value_len].to_string();
                match key.as_str() {
                    "allowedPowerups" => {
                        powerups = value
                            .split(',')
                            .filter_map(|entry| entry.parse::<i32>().ok())
                            .filter(|entry| lobby.allows_powerup(*entry))
                            .collect();
                    }
                    "name" => {
                        name = string_manager::filter_swearwords(&self.state, &value).await;
                    }
                    _ => {}
                }
                remaining = &remaining[2 + value_len..];
            }
        }

        if map_id < 0 || team_amount == 0 || name.is_empty() {
            return Ok(());
        }

        let runtime = self.state.runtime_config.read().await.clone();
        let player = self.build_game_player().await;
        let game_id = lobby.create_game(
            player,
            name,
            map_id,
            team_amount,
            powerups,
            if is_battle_ball {
                runtime.game_battle_ball_game_length_seconds
            } else {
                120
            },
            runtime.game_countdown_seconds,
            runtime.game_score_window_restart_game_seconds,
        );
        room_manager::save_room(&self.state, room).await;
        self.current_game_id = Some(game_id);
        self.current_game_room_id = Some(self.current_room_id);
        self.current_game_room_is_public = self.current_room_is_public;
        self.current_game_team_id = 0;
        Ok(())
    }

    async fn game_lobby_switch_team(&mut self, packet: &str) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        let tickets = self.user_tickets().await;
        if tickets <= 1 {
            self.send("ClJ")?;
            return Ok(());
        }

        let mut args = packet.get(2..).unwrap_or_default();
        let (_, used1) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        args = &args[used1..];
        let (team_id, _) = match decode_vl64(args) {
            Ok((value, used)) => (value as usize, used),
            Err(_) => return Ok(()),
        };

        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let Some(lobby) = room.lobby.as_mut() else {
            return Ok(());
        };
        let game_points = self.user_game_points(lobby.is_battle_ball).await;
        if !lobby.valid_gamerank(game_points) {
            self.send("ClK")?;
            return Ok(());
        }

        let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id) else {
            self.clear_current_game_state(false);
            return Ok(());
        };
        if game.state != crate::games::game::GameState::Waiting {
            return Ok(());
        }
        if !game.team_has_space(team_id) {
            self.send("ClH")?;
            return Ok(());
        }

        if self.current_game_team_id == -1 {
            game.remove_subviewer_by_user_id(self.user_id()?);
        }
        if self.current_game_team_id == -1 {
            let mut player = self.build_game_player().await;
            player.team_id = team_id as i32;
            if team_id < game.teams.len() {
                game.teams[team_id].push(player);
            }
        } else {
            game.move_player_by_user_id(
                self.user_id()?,
                Some(self.current_game_team_id as usize),
                Some(team_id),
            );
        }
        let payload = game.sub_payload();
        room_manager::save_room(&self.state, room).await;
        self.current_game_team_id = team_id as i32;
        self.broadcast_game_packet(&format!("Ci{}", payload))
            .await?;
        Ok(())
    }

    async fn game_lobby_leave(&mut self) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if game_room_id <= 0 {
            self.clear_current_game_state(false);
            return Ok(());
        }

        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let mut waiting_abort_recipients = Vec::new();
        let mut team_move_payload = None;
        let mut send_fs = false;
        let mut send_cmh = false;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(index) = lobby.games.iter().position(|entry| entry.id == game_id)
        {
            let is_owner = lobby.games[index].owner.user_id == self.user_id()?;
            if is_owner {
                if lobby.games[index].state == crate::games::game::GameState::Waiting {
                    for team in &lobby.games[index].teams {
                        waiting_abort_recipients.extend(team.iter().map(|entry| entry.user_id));
                    }
                    waiting_abort_recipients.extend(
                        lobby.games[index]
                            .subviewers
                            .iter()
                            .map(|entry| entry.user_id),
                    );
                }
                lobby.games.remove(index);
                send_fs = true;
            } else if self.current_game_team_id >= 0 {
                lobby.games[index].move_player_by_user_id(
                    self.user_id()?,
                    Some(self.current_game_team_id as usize),
                    None,
                );
                team_move_payload = Some(lobby.games[index].sub_payload());
            } else {
                lobby.games[index].remove_subviewer_by_user_id(self.user_id()?);
                send_cmh = true;
                send_fs = true;
            }
        }
        room_manager::save_room(&self.state, room).await;

        if let Some(payload) = team_move_payload {
            self.broadcast_game_packet(&format!("Ci{}", payload))
                .await?;
        }

        waiting_abort_recipients.sort_unstable();
        waiting_abort_recipients.dedup();
        for user_id in waiting_abort_recipients {
            if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                let _ = user.sender.send("CmH".to_string());
            }
        }

        self.clear_current_game_state(false);
        if send_cmh {
            self.send("CmH")?;
        }
        if send_fs {
            self.send("FS")?;
        }
        Ok(())
    }

    async fn game_lobby_kick(&mut self, packet: &str) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        let room_uid = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if game_room_id <= 0 {
            self.clear_current_game_state(false);
            return Ok(());
        }

        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let mut kicked_user_id = None;
        let mut payload = None;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
        {
            if game.owner.user_id != self.user_id()? {
                return Ok(());
            }
            for team_index in 0..game.teams.len() {
                if let Some(member) = game.teams[team_index]
                    .iter()
                    .find(|entry| entry.room_uid == room_uid)
                    .cloned()
                {
                    kicked_user_id = Some(member.user_id);
                    game.move_player_by_user_id(member.user_id, Some(team_index), None);
                    payload = Some(game.sub_payload());
                    break;
                }
            }
        }
        room_manager::save_room(&self.state, room).await;
        if let Some(payload) = payload {
            self.broadcast_game_packet(&format!("Ci{}", payload))
                .await?;
        }
        if let Some(user_id) = kicked_user_id
            && let Some(user) = user_manager::get_user(&self.state, user_id).await
        {
            let _ = user.sender.send("ClRA".to_string());
        }
        Ok(())
    }

    async fn game_lobby_start(&mut self) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if game_room_id <= 0 {
            self.clear_current_game_state(false);
            return Ok(());
        }

        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let mut player_recipients = Vec::new();
        let mut subviewer_recipients = Vec::new();
        let sub_payload;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
        {
            if game.owner.user_id != self.user_id()? {
                return Ok(());
            }
            game.start_game(&self.state).await?;
            sub_payload = Some(game.sub_payload());
            subviewer_recipients.extend(game.subviewers.iter().map(|entry| entry.user_id));
            for team in &game.teams {
                player_recipients.extend(team.iter().map(|entry| entry.user_id));
            }
        } else {
            self.clear_current_game_state(false);
            return Ok(());
        }
        room_manager::save_room(&self.state, room).await;
        room_manager::spawn_game_cycle_if_needed(
            Arc::new((*self.state).clone()),
            game_room_id,
            game_id,
        )
        .await;

        if let Some(payload) = sub_payload {
            for user_id in subviewer_recipients {
                if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                    let _ = user.sender.send(format!("Ci{}", payload));
                }
            }
        }
        for user_id in player_recipients {
            if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                let _ = user.sender.send(format!("Cq{}", encode_vl64(-1)));
            }
        }
        Ok(())
    }

    async fn try_enter_game_arena(&mut self, room_id: i64, _is_publicroom: bool) -> Result<bool> {
        let Some(game_id) = self.current_game_id else {
            return Ok(false);
        };
        let current_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if current_room_id <= 0 {
            return Ok(false);
        }

        let mut room = room_manager::load_room(
            &self.state,
            current_room_id,
            self.current_game_room_is_public,
        )
        .await?;
        let Some((is_battle_ball, map_id, map_packet)) = room.lobby.as_ref().and_then(|lobby| {
            lobby
                .games
                .iter()
                .find(|entry| entry.id == game_id)
                .and_then(|game| {
                    game.teams
                        .iter()
                        .flat_map(|team| team.iter())
                        .find(|entry| entry.user_id == self.user_id().unwrap_or(0))
                        .filter(|entry| entry.entering_game)
                        .map(|_| {
                            (
                                game.is_battle_ball,
                                game.map_id,
                                game.get_map(game.left_countdown_seconds, game.countdown_seconds),
                            )
                        })
                })
        }) else {
            return Ok(false);
        };

        if let Some(room_uid) = self.current_room_uid {
            room.remove_room_user(room_uid);
            room_manager::update_room_visitor_count(
                &self.state,
                current_room_id,
                room.users.len() as i64,
            )
            .await?;
            room_manager::save_room(&self.state, room).await;
        }

        self.clear_room_access_markers_for_self().await?;
        self.current_room_uid = None;
        self.current_room_id = 0;
        self.current_room_is_public = false;
        self.room_access_primary_ok = false;
        self.room_access_secondary_ok = false;
        self.is_owner = false;
        self.has_rights = false;

        let arena_type = if is_battle_ball { "bb" } else { "ss" };
        self.send(&format!("AE{}_arena_{} {}", arena_type, map_id, room_id))?;
        self.send(&format!("Cs{}", map_packet))?;

        if let Some(user_id) = self.logged_in_user_id
            && let Some(mut user) = user_manager::get_user(&self.state, user_id).await
        {
            user.in_room = false;
            user.room_id = 0;
            user.room_is_public = false;
            self.state.online_users.write().await.insert(user_id, user);
        }

        Ok(true)
    }

    async fn try_load_game_arena(&mut self) -> Result<bool> {
        let Some(game_id) = self.current_game_id else {
            return Ok(false);
        };
        if self.current_game_room_id.unwrap_or(0) <= 0 {
            return Ok(false);
        }

        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let mut arena_payload = None;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
        {
            let left_countdown_seconds = game.left_countdown_seconds;
            let countdown_seconds = game.countdown_seconds;
            let players_payload = game.get_players(left_countdown_seconds, countdown_seconds);
            let heightmap = game.heightmap.clone();
            if let Some(player) = game
                .teams
                .iter_mut()
                .flat_map(|team| team.iter_mut())
                .find(|entry| entry.user_id == self.user_id().unwrap_or(0))
            {
                if self.current_game_team_id >= 0 && player.entering_game {
                    arena_payload = Some((heightmap, players_payload));
                }
                player.entering_game = false;
            }
        } else {
            self.clear_current_game_state(false);
        }

        let should_send = arena_payload.is_some();
        if let Some((heightmap, players_payload)) = arena_payload {
            self.send(&format!("@_{}", heightmap))?;
            self.send(&format!("Cs{}", players_payload))?;
        }
        room_manager::save_room(&self.state, room).await;
        Ok(should_send)
    }

    async fn game_ingame_move(&mut self, packet: &str) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        if self.current_game_team_id < 0 {
            return Ok(());
        }
        let args = packet.get(3..).unwrap_or_default();
        let (goal_x, used) = match decode_vl64(args) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };
        let (goal_y, _) = match decode_vl64(&args[used..]) {
            Ok(result) => result,
            Err(_) => return Ok(()),
        };

        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        let mut room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        if let Some(lobby) = room.lobby.as_mut()
            && let Some(game) = lobby.games.iter_mut().find(|entry| entry.id == game_id)
        {
            if game.state != crate::games::game::GameState::Started {
                return Ok(());
            }
            for team in &mut game.teams {
                if let Some(player) = team
                    .iter_mut()
                    .find(|entry| entry.user_id == self.user_id().unwrap_or(0))
                {
                    player.goal_x = goal_x;
                    player.goal_y = goal_y;
                    break;
                }
            }
        } else {
            self.clear_current_game_state(false);
            return Ok(());
        }
        room_manager::save_room(&self.state, room).await;
        Ok(())
    }

    async fn game_ingame_replay_request(&mut self) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        if self.current_game_team_id < 0 {
            return Ok(());
        }

        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        let room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let mut recipients = Vec::new();
        if let Some(lobby) = room.lobby.as_ref()
            && let Some(game) = lobby.games.iter().find(|entry| entry.id == game_id)
        {
            if game.state != crate::games::game::GameState::Ended {
                return Ok(());
            }
            for team in &game.teams {
                recipients.extend(team.iter().map(|entry| entry.user_id));
            }
            recipients.extend(game.subviewers.iter().map(|entry| entry.user_id));
        } else {
            self.clear_current_game_state(false);
            return Ok(());
        }

        let packet = format!(
            "BK{} wants to replay!",
            self.username.clone().unwrap_or_default()
        );
        for user_id in recipients {
            if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                let _ = user.sender.send(packet.clone());
            }
        }
        Ok(())
    }

    fn clear_current_game_state(&mut self, clear_room: bool) {
        self.current_game_id = None;
        self.current_game_room_id = None;
        self.current_game_room_is_public = false;
        self.current_game_team_id = -1;
        if clear_room {
            self.current_room_id = 0;
            self.current_room_is_public = false;
            self.current_room_uid = None;
        }
    }

    async fn room_spin_wheel_of_fortune(&mut self, packet: &str) -> Result<()> {
        if !self.has_rights
            || self.current_room_id <= 0
            || self.current_room_is_public
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item) = room.wall_items.iter_mut().find(|entry| entry.id == item_id) else {
            return Ok(());
        };
        let sprite = item.sprite(&self.state).await;
        if sprite != "habbowheel" {
            return Ok(());
        }

        let rnd_num = (chrono::Local::now().timestamp_subsec_millis() % 10) as i32;
        let wall_position = item.wall_position.clone();
        item.var = rnd_num.to_string();
        self.state
            .db
            .run_query(&format!(
                "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                rnd_num, item_id
            ))
            .await?;
        room_manager::save_room(&self.state, room.clone()).await;

        let start_packet =
            room_manager::refresh_wallitem_packet(item_id, "habbowheel", &wall_position, "-1");
        let stop_packet = room_manager::refresh_wallitem_packet(
            item_id,
            "habbowheel",
            &wall_position,
            &rnd_num.to_string(),
        );
        self.broadcast_room_users(&room, &start_packet, None).await;
        tokio::spawn(broadcast_room_payload_after_delay(
            Arc::new((*self.state).clone()),
            self.current_room_id,
            stop_packet,
            4250,
        ));
        Ok(())
    }

    async fn room_activate_love_shuffler(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0
            || self.current_room_is_public
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }

        let item_id = match decode_vl64(packet.get(2..).unwrap_or_default()) {
            Ok((value, _)) => value as i64,
            Err(_) => return Ok(()),
        };
        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let Some(item) = room
            .floor_items
            .iter_mut()
            .find(|entry| entry.id == item_id)
        else {
            return Ok(());
        };
        let sprite = item.sprite(&self.state).await;
        if sprite != "val_randomizer" {
            return Ok(());
        }

        let rnd_num = ((chrono::Local::now().timestamp_subsec_millis() % 4) + 1) as i32;
        item.var = rnd_num.to_string();
        self.state
            .db
            .run_query(&format!(
                "UPDATE furniture SET var = '{}' WHERE id = '{}' LIMIT 1",
                rnd_num, item_id
            ))
            .await?;
        room_manager::save_room(&self.state, room.clone()).await;

        self.broadcast_room_users(&room, &format!("AX{}\u{2}123456789\u{2}", item_id), None)
            .await;
        tokio::spawn(broadcast_room_payload_after_delay(
            Arc::new((*self.state).clone()),
            self.current_room_id,
            format!("AX{}\u{2}{}\u{2}", item_id, rnd_num),
            5000,
        ));
        Ok(())
    }

    async fn trade_start(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0
            || self.current_room_is_public
            || self.current_room_uid.is_none()
        {
            return Ok(());
        }
        if !self.state.runtime_config.read().await.enable_trading {
            self.send(&format!(
                "BK{}",
                string_manager::get_string(&self.state, "trading_disabled").await?
            ))?;
            return Ok(());
        }

        let partner_room_uid = packet
            .get(2..)
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .unwrap_or(-1);
        if partner_room_uid < 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        if self.state.active_trades.read().await.contains_key(&user_id) {
            return Ok(());
        }
        let mut room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        let Some(partner) = room
            .users
            .iter()
            .find(|entry| entry.room_uid == partner_room_uid)
            .cloned()
        else {
            return Ok(());
        };
        if partner.user_id == user_id || partner.status_manager.contains_status("trd") {
            return Ok(());
        }

        let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
            return Ok(());
        };
        room.users[index].status_manager.add_status("trd", "");
        room.users[index].status_manager.remove_status("mv");
        if let Some(partner_index) = room
            .users
            .iter()
            .position(|entry| entry.user_id == partner.user_id)
        {
            room.users[partner_index]
                .status_manager
                .add_status("trd", "");
            room.users[partner_index].status_manager.remove_status("mv");
        }

        let user_status_packet = room.user_status_packet(user_id);
        let partner_status_packet = room.user_status_packet(partner.user_id);
        let recipients = room
            .users
            .iter()
            .map(|entry| entry.user_id)
            .collect::<Vec<_>>();
        room_manager::save_room(&self.state, room).await;

        {
            let mut trades = self.state.active_trades.write().await;
            trades.insert(
                user_id,
                crate::core::state::TradeState {
                    partner_user_id: partner.user_id,
                    partner_room_uid,
                    accepted: false,
                    item_ids: Vec::new(),
                },
            );
            trades.insert(
                partner.user_id,
                crate::core::state::TradeState {
                    partner_user_id: user_id,
                    partner_room_uid: self.current_room_uid.unwrap_or(-1),
                    accepted: false,
                    item_ids: Vec::new(),
                },
            );
        }

        if let Some(packet) = user_status_packet {
            for target_user_id in &recipients {
                if let Some(target) = user_manager::get_user(&self.state, *target_user_id).await {
                    let _ = target.sender.send(packet.clone());
                }
            }
        }
        if let Some(packet) = partner_status_packet {
            for target_user_id in &recipients {
                if let Some(target) = user_manager::get_user(&self.state, *target_user_id).await {
                    let _ = target.sender.send(packet.clone());
                }
            }
        }

        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), user_id).await;
        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), partner.user_id).await;
        Ok(())
    }

    async fn trade_offer_item(&mut self, packet: &str) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }

        let item_id = packet
            .get(2..)
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .unwrap_or(0);
        if item_id <= 0 {
            return Ok(());
        }

        let user_id = self.user_id()?;
        let (partner_user_id, partner_room_uid) = {
            let trades = self.state.active_trades.read().await;
            let Some(trade) = trades.get(&user_id) else {
                return Ok(());
            };
            (trade.partner_user_id, trade.partner_room_uid)
        };
        if partner_user_id <= 0 || partner_room_uid < 0 {
            return Ok(());
        }
        let room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        if !room
            .users
            .iter()
            .any(|entry| entry.room_uid == partner_room_uid)
        {
            return Ok(());
        }

        let template_id = self
            .state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tid FROM furniture WHERE id = '{}' AND ownerid = '{}' AND roomid = '0' LIMIT 1",
                item_id, user_id
            ))
            .await;
        if template_id <= 0 {
            return Ok(());
        }

        {
            let mut trades = self.state.active_trades.write().await;
            let Some(trade) = trades.get_mut(&user_id) else {
                return Ok(());
            };
            if trade.item_ids.len() >= 65 || trade.item_ids.contains(&item_id) {
                return Ok(());
            }
            trade.item_ids.push(item_id);
            trade.accepted = false;
            if let Some(partner_trade) = trades.get_mut(&partner_user_id) {
                partner_trade.accepted = false;
            }
        }

        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), user_id).await;
        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), partner_user_id).await;
        Ok(())
    }

    async fn trade_decline(&mut self) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }
        let user_id = self.user_id()?;
        let (partner_user_id, partner_room_uid) = {
            let trades = self.state.active_trades.read().await;
            let Some(trade) = trades.get(&user_id) else {
                return Ok(());
            };
            (trade.partner_user_id, trade.partner_room_uid)
        };
        if partner_room_uid < 0 {
            return Ok(());
        }
        let room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        if !room
            .users
            .iter()
            .any(|entry| entry.room_uid == partner_room_uid)
        {
            return Ok(());
        }

        {
            let mut trades = self.state.active_trades.write().await;
            if let Some(trade) = trades.get_mut(&user_id) {
                trade.accepted = false;
            }
            if let Some(partner_trade) = trades.get_mut(&partner_user_id) {
                partner_trade.accepted = false;
            }
        }

        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), user_id).await;
        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), partner_user_id).await;
        Ok(())
    }

    async fn trade_accept(&mut self) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }
        let user_id = self.user_id()?;
        let (partner_user_id, partner_room_uid) = {
            let trades = self.state.active_trades.read().await;
            let Some(trade) = trades.get(&user_id) else {
                return Ok(());
            };
            (trade.partner_user_id, trade.partner_room_uid)
        };
        if partner_room_uid < 0 {
            return Ok(());
        }
        let room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        if !room
            .users
            .iter()
            .any(|entry| entry.room_uid == partner_room_uid)
        {
            return Ok(());
        }

        let (user_items, partner_items, partner_accepted) = {
            let mut trades = self.state.active_trades.write().await;
            let partner_accepted = trades
                .get(&partner_user_id)
                .map(|trade| trade.accepted)
                .unwrap_or(false);
            let partner_items = trades
                .get(&partner_user_id)
                .map(|trade| trade.item_ids.clone())
                .unwrap_or_default();
            let Some(trade) = trades.get_mut(&user_id) else {
                return Ok(());
            };
            trade.accepted = true;
            let user_items = trade.item_ids.clone();
            (user_items, partner_items, partner_accepted)
        };

        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), user_id).await;
        refresh_trade_boxes_for_user(Arc::new((*self.state).clone()), partner_user_id).await;

        if partner_accepted {
            for item_id in &user_items {
                if *item_id > 0 {
                    self.state
                        .db
                        .run_query(&format!(
                            "UPDATE furniture SET ownerid = '{}',roomid = '0' WHERE id = '{}' LIMIT 1",
                            partner_user_id, item_id
                        ))
                        .await?;
                }
            }
            for item_id in &partner_items {
                if *item_id > 0 {
                    self.state
                        .db
                        .run_query(&format!(
                            "UPDATE furniture SET ownerid = '{}',roomid = '0' WHERE id = '{}' LIMIT 1",
                            user_id, item_id
                        ))
                        .await?;
                }
            }

            self.trade_abort(false).await?;
        }

        Ok(())
    }

    async fn trade_abort(&mut self, refresh_self_hand: bool) -> Result<()> {
        if self.current_room_id <= 0 || self.current_room_uid.is_none() {
            return Ok(());
        }
        let user_id = self.user_id()?;
        let partner_room_uid = {
            let trades = self.state.active_trades.read().await;
            let Some(trade) = trades.get(&user_id) else {
                return Ok(());
            };
            trade.partner_room_uid
        };
        if partner_room_uid < 0 {
            return Ok(());
        }
        let room = room_manager::load_room(&self.state, self.current_room_id, false).await?;
        if !room
            .users
            .iter()
            .any(|entry| entry.room_uid == partner_room_uid)
        {
            return Ok(());
        }
        abort_trade_for_user(Arc::new((*self.state).clone()), user_id, refresh_self_hand).await;
        Ok(())
    }

    async fn build_game_player(&self) -> GamePlayer {
        let mut player = GamePlayer::new(
            self.user_id().unwrap_or(0),
            self.username.clone().unwrap_or_default(),
        );
        player.mission = self.mission.clone().unwrap_or_default();
        player.figure = self.figure.clone().unwrap_or_default();
        player.sex = self.sex.clone().unwrap_or_else(|| "M".to_string());
        player.room_uid = self.current_room_uid.unwrap_or(self.user_id().unwrap_or(0));
        player.team_id = self.current_game_team_id;
        player
    }

    async fn user_tickets(&self) -> i64 {
        self.state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                self.user_id().unwrap_or(0)
            ))
            .await
    }

    async fn user_game_points(&self, is_battle_ball: bool) -> i64 {
        let column = if is_battle_ball {
            "bb_totalpoints"
        } else {
            "ss_totalpoints"
        };
        self.state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT {} FROM users WHERE id = '{}' LIMIT 1",
                column,
                self.user_id().unwrap_or(0)
            ))
            .await
    }

    async fn config_int(&self, key: &str, fallback: i64) -> i64 {
        self.state
            .db
            .run_read_i64(&format!(
                "SELECT sval FROM system_config WHERE skey = '{}' LIMIT 1",
                Database::stripslash(key)
            ))
            .await
            .ok()
            .flatten()
            .unwrap_or(fallback)
    }

    async fn user_is_muted(&self) -> bool {
        self.user_muted_state(self.user_id().unwrap_or_default())
            .await
            .unwrap_or(false)
    }

    async fn stop_chat_typing_for_speech_command(&mut self) -> Result<()> {
        if let Some(room_uid) = self.current_room_uid {
            let mut room = room_manager::load_room(
                &self.state,
                self.current_room_id,
                self.current_room_is_public,
            )
            .await?;
            if let Some(user) = room
                .users
                .iter_mut()
                .find(|entry| entry.room_uid == room_uid)
                && user.is_typing
            {
                user.is_typing = false;
                let recipients = room
                    .users
                    .iter()
                    .map(|entry| entry.user_id)
                    .collect::<Vec<_>>();
                room_manager::save_room(&self.state, room).await;
                let packet = format!("FO{}H", encode_vl64(room_uid as i32));
                for target_user_id in recipients {
                    if let Some(user) = user_manager::get_user(&self.state, target_user_id).await {
                        let _ = user.sender.send(packet.clone());
                    }
                }
            }
            return Ok(());
        }

        if self.current_game_id.is_some()
            && let Some(room_uid) = self.current_game_player_room_uid().await?
        {
            self.broadcast_game_packet(&format!("FO{}H", encode_vl64(room_uid as i32)))
                .await?;
        }
        Ok(())
    }

    async fn current_game_player_room_uid(&self) -> Result<Option<i64>> {
        let Some(game_id) = self.current_game_id else {
            return Ok(None);
        };
        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if game_room_id <= 0 {
            return Ok(None);
        }
        let room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let room_uid = room
            .lobby
            .as_ref()
            .and_then(|lobby| lobby.games.iter().find(|entry| entry.id == game_id))
            .and_then(|game| {
                game.teams
                    .iter()
                    .flat_map(|team| team.iter())
                    .find(|entry| entry.user_id == self.user_id().unwrap_or_default())
                    .map(|entry| entry.room_uid)
            });
        Ok(room_uid)
    }

    async fn broadcast_game_packet(&self, payload: &str) -> Result<()> {
        let Some(game_id) = self.current_game_id else {
            return Ok(());
        };
        let game_room_id = self.current_game_room_id.unwrap_or(self.current_room_id);
        if game_room_id <= 0 {
            return Ok(());
        }
        let room =
            room_manager::load_room(&self.state, game_room_id, self.current_game_room_is_public)
                .await?;
        let Some(game) = room
            .lobby
            .as_ref()
            .and_then(|lobby| lobby.games.iter().find(|entry| entry.id == game_id))
        else {
            return Ok(());
        };

        let mut recipients = Vec::new();
        if game.state == crate::games::game::GameState::Waiting {
            recipients.extend(game.subviewers.iter().map(|entry| entry.user_id));
        }
        for team in &game.teams {
            recipients.extend(team.iter().map(|entry| entry.user_id));
        }
        recipients.sort_unstable();
        recipients.dedup();

        for target_user_id in recipients {
            if let Some(user) = user_manager::get_user(&self.state, target_user_id).await {
                let _ = user.sender.send(payload.to_string());
            }
        }
        Ok(())
    }

    async fn user_muted_state(&self, user_id: i64) -> Option<bool> {
        user_manager::get_user(&self.state, user_id)
            .await
            .map(|user| user.is_muted.load(Ordering::Relaxed))
    }

    async fn set_user_muted(&self, user_id: i64, muted: bool) {
        if let Some(user) = user_manager::get_user(&self.state, user_id).await {
            user.is_muted.store(muted, Ordering::Relaxed);
        }
    }

    async fn kick_online_user_from_current_room(
        &self,
        target_id: i64,
        message: &str,
    ) -> Result<()> {
        let Some(target_user) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(());
        };
        if !target_user.in_room || target_user.room_id <= 0 {
            return Ok(());
        }

        let mut room =
            room_manager::load_room(&self.state, target_user.room_id, target_user.room_is_public)
                .await?;
        let Some(target) = room.kick_user(target_id) else {
            return Ok(());
        };
        let outcome = crate::virtuals::rooms::virtual_room::RoomKickOutcome {
            targets: vec![target],
        };
        self.apply_room_kick_outcome_for_room(target_user.room_id, room, outcome, message)
            .await
    }

    async fn kick_user_from_room(&self, target_id: i64, message: &str) -> Result<bool> {
        let Some(target_user) = user_manager::get_user(&self.state, target_id).await else {
            return Ok(false);
        };
        if !target_user.in_room || target_user.room_id <= 0 {
            return Ok(false);
        }
        let mut room =
            room_manager::load_room(&self.state, target_user.room_id, target_user.room_is_public)
                .await?;
        let Some(target) = room.kick_user(target_id) else {
            return Ok(false);
        };
        let outcome = crate::virtuals::rooms::virtual_room::RoomKickOutcome {
            targets: vec![target],
        };
        self.apply_room_kick_outcome_for_room(target_user.room_id, room, outcome, message)
            .await?;
        Ok(true)
    }

    async fn apply_room_kick_outcome(
        &self,
        room: crate::virtuals::rooms::virtual_room::VirtualRoom,
        outcome: crate::virtuals::rooms::virtual_room::RoomKickOutcome,
        message: &str,
    ) -> Result<()> {
        self.apply_room_kick_outcome_for_room(self.current_room_id, room, outcome, message)
            .await
    }

    async fn apply_room_kick_outcome_for_room(
        &self,
        room_id: i64,
        room: crate::virtuals::rooms::virtual_room::VirtualRoom,
        outcome: crate::virtuals::rooms::virtual_room::RoomKickOutcome,
        message: &str,
    ) -> Result<()> {
        apply_room_kick_outcome_for_room(&self.state, room_id, room, outcome, message).await
    }

    async fn room_kick_lower_rank_users(&self, message: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let mut room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let outcome = room.kick_users(self.user_id().unwrap_or_default(), self.rank);
        self.apply_room_kick_outcome(room, outcome, message).await
    }

    async fn room_set_mute_lower_rank_users(&self, muted: bool, message: &str) -> Result<()> {
        if self.current_room_id <= 0 {
            return Ok(());
        }

        let room = room_manager::load_room(
            &self.state,
            self.current_room_id,
            self.current_room_is_public,
        )
        .await?;
        let outcome = room.mute_users(self.user_id().unwrap_or_default(), self.rank);
        for user_id in outcome.user_ids {
            self.set_user_muted(user_id, muted).await;
            if let Some(user) = user_manager::get_user(&self.state, user_id).await {
                let packet = if muted {
                    format!(
                        "BK{}\r{}",
                        string_manager::get_string(&self.state, "scommand_muted")
                            .await
                            .unwrap_or_else(|_| "scommand_muted".to_string()),
                        message
                    )
                } else {
                    format!(
                        "BK{}",
                        string_manager::get_string(&self.state, "scommand_unmuted")
                            .await
                            .unwrap_or_else(|_| "scommand_unmuted".to_string())
                    )
                };
                let _ = user.sender.send(packet);
            }
        }
        Ok(())
    }

    fn user_id(&self) -> Result<i64> {
        self.logged_in_user_id.context("user not logged in")
    }

    fn send(&self, payload: &str) -> Result<()> {
        self.tx
            .send(payload.to_string())
            .map_err(|_| anyhow::anyhow!("session writer closed"))
    }

    async fn mark_ping_ok(&self) {
        if let Some(user_id) = self.logged_in_user_id
            && let Some(user) = user_manager::get_user(&self.state, user_id).await
        {
            user.ping_ok.store(true, Ordering::Relaxed);
        }
    }

    async fn broadcast_room_users(
        &self,
        room: &crate::virtuals::rooms::virtual_room::VirtualRoom,
        payload: &str,
        except_user_id: Option<i64>,
    ) {
        for room_user in &room.users {
            if except_user_id == Some(room_user.user_id) {
                continue;
            }

            if room_user.user_id == self.logged_in_user_id.unwrap_or_default() {
                let _ = self.send(payload);
                continue;
            }

            if let Some(user) = user_manager::get_user(&self.state, room_user.user_id).await {
                let _ = user.sender.send(payload.to_string());
            }
        }
    }

    async fn cleanup(&self) {
        if self.current_room_id > 0
            && let Some(room_uid) = self.current_room_uid
            && let Ok(mut room) = room_manager::load_room(
                &self.state,
                self.current_room_id,
                self.current_room_is_public,
            )
            .await
        {
            room.remove_room_user(room_uid);
            self.broadcast_room_users(&room, &format!("@]{room_uid}"), self.logged_in_user_id)
                .await;
            let visitor_count = room.users.len() as i64;
            if visitor_count > 0 {
                let _ = room_manager::update_room_visitor_count(
                    &self.state,
                    self.current_room_id,
                    visitor_count,
                )
                .await;
                room_manager::save_room(&self.state, room).await;
            } else {
                let _ = room_manager::remove_room(&self.state, self.current_room_id).await;
            }
        }

        if let Some(user_id) = self.logged_in_user_id {
            self.state.clear_doorbell_access(user_id).await;
            self.state.clear_doorbell_denied(user_id).await;
            abort_trade_for_user(Arc::new((*self.state).clone()), user_id, false).await;
            user_manager::remove_user_if_connection(&self.state, user_id, self.connection_id).await;
        }
        self.state.free_connection_id(self.connection_id).await;
    }
}

async fn writer_loop(
    mut writer: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<String>,
) -> Result<()> {
    while let Some(payload) = rx.recv().await {
        debug!(payload = %payload, "sending packet");
        writer.write_all(&legacy_frame(&payload)).await?;
    }
    writer.shutdown().await?;
    Ok(())
}

async fn broadcast_room_payload_after_delay(
    state: Arc<AppState>,
    room_id: i64,
    payload: String,
    delay_ms: u64,
) {
    sleep(Duration::from_millis(delay_ms)).await;

    let Some(room) = room_manager::get_room(&state, room_id).await else {
        return;
    };
    for room_user in &room.users {
        if let Some(user) = user_manager::get_user(&state, room_user.user_id).await {
            let _ = user.sender.send(payload.clone());
        }
    }
}

async fn remove_status_after_delay(
    state: Arc<AppState>,
    room_id: i64,
    user_id: i64,
    key: String,
    delay_ms: u64,
) {
    sleep(Duration::from_millis(delay_ms)).await;

    let Some(mut room) = room_manager::get_room(&state, room_id).await else {
        return;
    };
    let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
        return;
    };

    room.users[index].status_manager.remove_status(&key);
    let Some(status_packet) = room.user_status_packet(user_id) else {
        return;
    };
    let recipients = room
        .users
        .iter()
        .map(|entry| entry.user_id)
        .collect::<Vec<_>>();
    room_manager::save_room(&state, room).await;

    for target_user_id in recipients {
        if let Some(user) = user_manager::get_user(&state, target_user_id).await {
            let _ = user.sender.send(status_packet.clone());
        }
    }
}

async fn run_carry_item_cycle(
    state: Arc<AppState>,
    room_id: i64,
    user_id: i64,
    item: String,
    sip_amount: usize,
    sip_interval_ms: u64,
    sip_duration_ms: u64,
) {
    for _ in 0..sip_amount {
        if !update_room_user_statuses(&state, room_id, user_id, |user| {
            user.status_manager.add_status("carryd", &item);
            user.status_manager.remove_status("drink");
        })
        .await
        {
            return;
        }

        sleep(Duration::from_millis(sip_interval_ms)).await;

        if !update_room_user_statuses(&state, room_id, user_id, |user| {
            user.status_manager.remove_status("carryd");
            user.status_manager.add_status("drink", &item);
        })
        .await
        {
            return;
        }

        sleep(Duration::from_millis(sip_duration_ms)).await;
    }

    let _ = update_room_user_statuses(&state, room_id, user_id, |user| {
        user.status_manager.drop_carryd_item();
    })
    .await;
}

async fn run_same_room_teleporter_use(
    state: Arc<AppState>,
    room_id: i64,
    user_id: i64,
    teleporter_1_id: i64,
    teleporter_2_id: i64,
    username: String,
    sprite: String,
) {
    sleep(Duration::from_millis(500)).await;

    let Some(mut room) = room_manager::get_room(&state, room_id).await else {
        return;
    };
    let Some(teleporter_2) = room.floor_item(teleporter_2_id).cloned() else {
        return;
    };
    if !room.teleport_user_to_floor_item(user_id, &teleporter_2) {
        room_manager::save_room(&state, room).await;
        return;
    }

    let Some(status_packet) = room.user_status_packet(user_id) else {
        room_manager::save_room(&state, room).await;
        return;
    };
    let recipients = room
        .users
        .iter()
        .map(|entry| entry.user_id)
        .collect::<Vec<_>>();
    room_manager::save_room(&state, room).await;

    let flash_packet = format!("AY{}/{}/{}", teleporter_1_id, username, sprite);
    let arrive_packet = format!("A\\{}/{}/{}", teleporter_2_id, username, sprite);
    for target_user_id in recipients {
        if let Some(user) = user_manager::get_user(&state, target_user_id).await {
            let _ = user.sender.send(flash_packet.clone());
            let _ = user.sender.send(arrive_packet.clone());
            let _ = user.sender.send(status_packet.clone());
        }
    }
}

async fn refresh_trade_boxes_for_user(state: Arc<AppState>, user_id: i64) {
    let Some(user) = user_manager::get_user(&state, user_id).await else {
        return;
    };
    let Some(trade) = state.active_trades.read().await.get(&user_id).cloned() else {
        return;
    };
    if user.room_id <= 0 {
        return;
    }
    let Some(room) = room_manager::get_room(&state, user.room_id).await else {
        return;
    };
    if !room.users.iter().any(|entry| entry.user_id == user_id)
        || !room
            .users
            .iter()
            .any(|entry| entry.room_uid == trade.partner_room_uid)
    {
        return;
    }
    let Some(partner) = user_manager::get_user(&state, trade.partner_user_id).await else {
        return;
    };
    let Some(partner_trade) = state
        .active_trades
        .read()
        .await
        .get(&trade.partner_user_id)
        .cloned()
    else {
        return;
    };

    let mut trade_boxes = format!(
        "Al{}\t{}\t",
        user.username,
        trade.accepted.to_string().to_lowercase()
    );
    if !trade.item_ids.is_empty() {
        trade_boxes.push_str(&catalogue_manager::trade_item_list(&state, &trade.item_ids).await);
    }
    trade_boxes.push_str(&format!(
        "\r{}\t{}\t",
        partner.username,
        partner_trade.accepted.to_string().to_lowercase()
    ));
    if !partner_trade.item_ids.is_empty() {
        trade_boxes
            .push_str(&catalogue_manager::trade_item_list(&state, &partner_trade.item_ids).await);
    }
    let _ = user.sender.send(trade_boxes);
}

async fn abort_trade_for_user(state: Arc<AppState>, user_id: i64, _refresh_self_hand: bool) {
    let trade = {
        let trades = state.active_trades.read().await;
        trades.get(&user_id).cloned()
    };
    let Some(trade) = trade else {
        return;
    };
    let partner_user_id = trade.partner_user_id;

    {
        let mut trades = state.active_trades.write().await;
        trades.remove(&user_id);
        trades.remove(&partner_user_id);
    }

    let room_id = user_manager::get_user(&state, user_id)
        .await
        .map(|user| user.room_id)
        .unwrap_or(0);
    if room_id <= 0 {
        return;
    }

    let Some(mut room) = room_manager::get_room(&state, room_id).await else {
        return;
    };
    if !room.users.iter().any(|entry| entry.user_id == user_id)
        || !room
            .users
            .iter()
            .any(|entry| entry.room_uid == trade.partner_room_uid)
    {
        return;
    }

    if let Some(user) = user_manager::get_user(&state, user_id).await {
        let _ = user.sender.send("An".to_string());
        refresh_hand_for_user(&state, user_id, "update").await;
    }
    if let Some(user) = user_manager::get_user(&state, partner_user_id).await {
        let _ = user.sender.send("An".to_string());
        refresh_hand_for_user(&state, partner_user_id, "update").await;
    }

    let mut packets = Vec::new();
    for target_user_id in [user_id, partner_user_id] {
        if let Some(index) = room
            .users
            .iter()
            .position(|entry| entry.user_id == target_user_id)
        {
            room.users[index].status_manager.remove_status("trd");
            if let Some(packet) = room.user_status_packet(target_user_id) {
                packets.push(packet);
            }
        }
    }
    let recipients = room
        .users
        .iter()
        .map(|entry| entry.user_id)
        .collect::<Vec<_>>();
    room_manager::save_room(&state, room).await;
    for packet in packets {
        for target_user_id in &recipients {
            if let Some(user) = user_manager::get_user(&state, *target_user_id).await {
                let _ = user.sender.send(packet.clone());
            }
        }
    }
}

pub(crate) async fn refresh_hand_for_user(state: &Arc<AppState>, user_id: i64, mode: &str) {
    let Some(user) = user_manager::get_user(state, user_id).await else {
        return;
    };
    let mut hand_page = user.hand_page.load(Ordering::Relaxed);
    let Ok(packet) = build_hand_packet(state, user_id, &mut hand_page, mode).await else {
        return;
    };
    user.hand_page.store(hand_page, Ordering::Relaxed);
    let _ = user.sender.send(packet);
}

async fn load_current_badges(state: &AppState, user_id: i64) -> [String; 5] {
    if !state
        .db
        .check_exists(&format!(
            "SELECT userid FROM users_badgescur WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
    {
        let _ = state
            .db
            .run_query(&format!(
                "INSERT INTO users_badgescur(userid,badge1,badge2,badge3,badge4,badge5) VALUES ('{}','','','','','')",
                user_id
            ))
            .await;
    }

    let row = state
        .db
        .run_read_row(&format!(
            "SELECT badge1,badge2,badge3,badge4,badge5 FROM users_badgescur WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
        .ok()
        .unwrap_or_default();
    std::array::from_fn(|index| row.get(index).cloned().unwrap_or_default())
}

async fn load_current_group_status(state: &AppState, user_id: i64) -> (i64, i32) {
    let group_id = state
        .db
        .run_read_unsafe_i64(&format!(
            "SELECT groupid FROM groups_memberships WHERE userid = '{}' AND is_current = '1' LIMIT 1",
            user_id
        ))
        .await;
    if group_id <= 0 {
        return (0, 0);
    }

    let group_member_rank = state
        .db
        .run_read_unsafe_i64(&format!(
            "SELECT member_rank FROM groups_memberships WHERE userid = '{}' AND groupid = '{}' LIMIT 1",
            user_id, group_id
        ))
        .await as i32;
    (group_id, group_member_rank)
}

async fn persist_current_badges(
    state: &AppState,
    user_id: i64,
    badges: &[String; 5],
) -> Result<()> {
    if !state
        .db
        .check_exists(&format!(
            "SELECT userid FROM users_badgescur WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await
    {
        state
            .db
            .run_query(&format!(
                "INSERT INTO users_badgescur(userid,badge1,badge2,badge3,badge4,badge5) VALUES ('{}','','','','','')",
                user_id
            ))
            .await?;
    }

    let badge1 = Database::stripslash(&badges[0]);
    let badge2 = Database::stripslash(&badges[1]);
    let badge3 = Database::stripslash(&badges[2]);
    let badge4 = Database::stripslash(&badges[3]);
    let badge5 = Database::stripslash(&badges[4]);
    state
        .db
        .run_query(&format!(
            "UPDATE users_badgescur SET badge1 = '{}',badge2 = '{}',badge3 = '{}',badge4 = '{}',badge5 = '{}' WHERE userid = '{}' LIMIT 1",
            badge1, badge2, badge3, badge4, badge5, user_id
        ))
        .await?;
    Ok(())
}

fn ip_matches_legacy_ticket_ip(expected_ip: &str, actual_ip: &str) -> bool {
    if expected_ip == actual_ip {
        return true;
    }

    let expected_is_loopback =
        is_loopback_ip_text(expected_ip) || expected_ip.eq_ignore_ascii_case("localhost");
    let actual_is_loopback =
        is_loopback_ip_text(actual_ip) || actual_ip.eq_ignore_ascii_case("localhost");
    expected_is_loopback && actual_is_loopback
}

fn is_loopback_ip_text(value: &str) -> bool {
    value
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn collect_current_badges(badges: &[String], slot_ids: &[i64]) -> [String; 5] {
    let mut current_badges = std::array::from_fn::<_, 5, _>(|_| String::new());
    for (index, badge) in badges.iter().enumerate() {
        let slot_id = *slot_ids.get(index).unwrap_or(&0);
        if let Ok(slot_index) = usize::try_from(slot_id - 1)
            && slot_id > 0
            && slot_index < current_badges.len()
        {
            current_badges[slot_index] = badge.clone();
        }
    }
    current_badges
}

fn speech_command_name(text: &str) -> Option<&str> {
    text.split_whitespace().next()
}

fn is_known_speech_command_name(name: &str) -> bool {
    matches!(
        name,
        "emptyhand"
            | "alert"
            | "roomalert"
            | "kick"
            | "roomkick"
            | "shutup"
            | "unmute"
            | "roomshutup"
            | "roomunmute"
            | "ban"
            | "superban"
            | "info"
            | "find"
            | "teleport"
            | "warp"
            | "teletome"
            | "givebadge"
            | "offline"
            | "position"
            | "ha"
            | "ra"
            | "refreshrooms"
            | "refreshhotel"
            | "refreshcatalogue"
            | "coins"
            | "commands"
    )
}

fn parse_swim_outfit_selection(packet: &str) -> String {
    Database::stripslash(packet.get(2..).unwrap_or_default())
}

fn game_ticket_offer(amount: i32) -> Option<(i64, i64)> {
    match amount {
        1 => Some((2, 1)),
        2 => Some((20, 6)),
        _ => None,
    }
}

fn is_legacy_teleporter_sprite(sprite: &str) -> bool {
    matches!(
        sprite,
        "door" | "doorB" | "doorC" | "doorD" | "teleport_door" | "xmas08_telep" | "ads_cltele"
    )
}

fn is_user_on_teleporter(user_x: i32, user_y: i32, teleporter_x: i32, teleporter_y: i32) -> bool {
    user_x == teleporter_x && user_y == teleporter_y
}

fn is_user_at_teleporter_entrance(
    user_x: i32,
    user_y: i32,
    teleporter_x: i32,
    teleporter_y: i32,
    teleporter_z: u8,
) -> bool {
    match teleporter_z {
        2 => user_x == teleporter_x + 1 && user_y == teleporter_y,
        4 => user_x == teleporter_x && user_y == teleporter_y + 1,
        _ => false,
    }
}

fn format_teleporter_arrival_packet(teleporter_id: i64, username: &str, sprite: &str) -> String {
    format!("A\\{}/{}/{}", teleporter_id, username, sprite)
}

#[cfg(test)]
mod tests {
    use super::{
        collect_current_badges, format_teleporter_arrival_packet, game_ticket_offer,
        has_local_room_state_for_test, ip_matches_legacy_ticket_ip, is_known_speech_command_name,
        is_user_at_teleporter_entrance, is_user_on_teleporter, parse_swim_outfit_selection,
        should_reconcile_room_state, speech_command_name,
    };

    #[test]
    fn recognizes_known_speech_commands() {
        assert_eq!(
            speech_command_name("roomalert hello there"),
            Some("roomalert")
        );
        assert!(is_known_speech_command_name("roomalert"));
        assert!(is_known_speech_command_name("refreshhotel"));
        assert!(is_known_speech_command_name("teleport"));
        assert!(is_known_speech_command_name("givebadge"));
        assert!(is_known_speech_command_name("warp"));
        assert!(!is_known_speech_command_name("dive-off"));
    }

    #[test]
    fn ignores_blank_speech_command_input() {
        assert_eq!(speech_command_name(""), None);
        assert_eq!(speech_command_name("   "), None);
    }

    #[test]
    fn allows_clearing_swim_outfit_back_to_normal_clothes() {
        assert_eq!(parse_swim_outfit_selection("At"), "");
    }

    #[test]
    fn detects_when_local_room_state_needs_reconciliation() {
        assert!(has_local_room_state_for_test(
            1, None, false, false, false, false, false
        ));
        assert!(has_local_room_state_for_test(
            0,
            Some(5),
            false,
            false,
            false,
            false,
            false
        ));
        assert!(!has_local_room_state_for_test(
            0, None, false, false, false, false, false
        ));
    }

    #[test]
    fn preserves_pending_room_entry_until_room_uid_exists() {
        assert!(!should_reconcile_room_state(None, false, 0));
    }

    #[test]
    fn reconciles_only_attached_room_state_when_online_user_is_not_in_room() {
        assert!(should_reconcile_room_state(Some(5), false, 0));
        assert!(!should_reconcile_room_state(Some(5), true, 0));
        assert!(!should_reconcile_room_state(Some(5), false, 12));
    }

    #[test]
    fn accepts_ipv4_and_ipv6_loopback_for_legacy_ticket_ip_check() {
        assert!(ip_matches_legacy_ticket_ip("::1", "127.0.0.1"));
        assert!(ip_matches_legacy_ticket_ip("127.0.0.1", "::1"));
        assert!(ip_matches_legacy_ticket_ip("localhost", "::1"));
        assert!(!ip_matches_legacy_ticket_ip("192.168.1.4", "::1"));
    }

    #[test]
    fn maps_enabled_badges_into_current_badge_slots() {
        let badges = vec![
            "ACH_ONE".to_string(),
            "ACH_TWO".to_string(),
            "ACH_THREE".to_string(),
        ];
        let slot_ids = vec![0, 3, 1];
        let current_badges = collect_current_badges(&badges, &slot_ids);

        assert_eq!(current_badges[0], "ACH_THREE");
        assert_eq!(current_badges[2], "ACH_TWO");
        assert!(current_badges[1].is_empty());
    }

    #[test]
    fn requires_exact_teleporter_tiles_and_entrances() {
        assert!(is_user_on_teleporter(4, 7, 4, 7));
        assert!(!is_user_on_teleporter(4, 8, 4, 7));
        assert!(!is_user_on_teleporter(5, 7, 4, 7));

        assert!(is_user_at_teleporter_entrance(5, 7, 4, 7, 2));
        assert!(!is_user_at_teleporter_entrance(5, 8, 4, 7, 2));
        assert!(is_user_at_teleporter_entrance(4, 8, 4, 7, 4));
        assert!(!is_user_at_teleporter_entrance(5, 8, 4, 7, 4));
    }

    #[test]
    fn teleporter_arrival_packet_uses_sprite_payload() {
        assert_eq!(
            format_teleporter_arrival_packet(42, "Jamie", "teleport_door"),
            "A\\42/Jamie/teleport_door"
        );
    }

    #[test]
    fn maps_legacy_game_ticket_offers() {
        assert_eq!(game_ticket_offer(1), Some((2, 1)));
        assert_eq!(game_ticket_offer(2), Some((20, 6)));
        assert_eq!(game_ticket_offer(3), None);
    }
}

#[cfg(test)]
fn has_local_room_state_for_test(
    current_room_id: i64,
    current_room_uid: Option<i64>,
    current_room_is_public: bool,
    room_access_primary_ok: bool,
    room_access_secondary_ok: bool,
    is_owner: bool,
    has_rights: bool,
) -> bool {
    current_room_id > 0
        || current_room_uid.is_some()
        || current_room_is_public
        || room_access_primary_ok
        || room_access_secondary_ok
        || is_owner
        || has_rights
}

fn should_reconcile_room_state(
    current_room_uid: Option<i64>,
    user_in_room: bool,
    user_room_id: i64,
) -> bool {
    current_room_uid.is_some() && !user_in_room && user_room_id <= 0
}

pub(crate) async fn build_hand_packet(
    state: &AppState,
    user_id: i64,
    hand_page: &mut i32,
    mode: &str,
) -> Result<String> {
    let item_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT id FROM furniture WHERE ownerid = '{}' AND roomid = '0' ORDER BY id ASC",
            user_id
        ))
        .await?;

    let item_count = item_ids.len() as i32;
    match mode {
        "next" => *hand_page += 1,
        "prev" => *hand_page -= 1,
        "last" => {
            *hand_page = if item_count > 0 {
                (item_count - 1) / 9
            } else {
                0
            };
        }
        "update" => {}
        _ => *hand_page = 0,
    }

    let mut hand = String::from("BL");
    if !item_ids.is_empty() {
        let mut start_id = ((*hand_page).max(0) * 9) as usize;
        let mut stop_id = item_ids.len();

        loop {
            if stop_id > start_id + 9 {
                stop_id = start_id + 9;
            }
            if start_id < stop_id {
                break;
            }
            *hand_page -= 1;
            if *hand_page < 0 {
                *hand_page = 0;
                start_id = 0;
                break;
            }
            start_id = (*hand_page * 9) as usize;
            stop_id = item_ids.len();
        }

        for (index, item_id) in item_ids[start_id..stop_id].iter().enumerate() {
            let absolute_index = start_id + index;
            let template_id = state
                .db
                .run_read_unsafe_i64(&format!(
                    "SELECT tid FROM furniture WHERE id = '{}' LIMIT 1",
                    item_id
                ))
                .await;
            let template = catalogue_manager::get_template(state, template_id).await;
            let recycleable = if template.is_recycleable { '1' } else { '0' };

            if template.type_id == 0 {
                let mut colour = template.colour.clone();
                if template.sprite == "post.it" || template.sprite == "post.it.vd" {
                    colour = state
                        .db
                        .run_read_unsafe_string(&format!(
                            "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                            item_id
                        ))
                        .await;
                }

                hand.push_str(&format!(
                    "SI{sep}{id}{sep}{slot}{sep}I{sep}{id}{sep}{sprite}{sep}{colour}{sep}{recycle}/",
                    sep = '\u{1e}',
                    id = item_id,
                    slot = absolute_index,
                    sprite = template.sprite,
                    colour = colour,
                    recycle = recycleable
                ));
            } else {
                let item_var = state
                    .db
                    .run_read_unsafe_string(&format!(
                        "SELECT var FROM furniture WHERE id = '{}' LIMIT 1",
                        item_id
                    ))
                    .await;
                hand.push_str(&format!(
                    "SI{sep}{id}{sep}{slot}{sep}S{sep}{id}{sep}{sprite}{sep}{length}{sep}{width}{sep}{var}{sep}{colour}{sep}{recycle}{sep}{sprite}{sep}/",
                    sep = '\u{1e}',
                    id = item_id,
                    slot = absolute_index,
                    sprite = template.sprite,
                    length = template.length,
                    width = template.width,
                    var = item_var,
                    colour = template.colour,
                    recycle = recycleable
                ));
            }
        }
    }

    hand.push('\r');
    hand.push_str(&item_ids.len().to_string());
    Ok(hand)
}

async fn update_room_user_statuses<F>(
    state: &Arc<AppState>,
    room_id: i64,
    user_id: i64,
    mutator: F,
) -> bool
where
    F: FnOnce(&mut VirtualRoomUser),
{
    let Some(mut room) = room_manager::get_room(state, room_id).await else {
        return false;
    };
    let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id) else {
        return false;
    };

    mutator(&mut room.users[index]);
    let Some(status_packet) = room.user_status_packet(user_id) else {
        return false;
    };
    let recipients = room
        .users
        .iter()
        .map(|entry| entry.user_id)
        .collect::<Vec<_>>();
    room_manager::save_room(state, room).await;

    for target_user_id in recipients {
        if let Some(user) = user_manager::get_user(state, target_user_id).await {
            let _ = user.sender.send(status_packet.clone());
        }
    }
    true
}

pub(crate) async fn mus_refresh_appearance(state: &Arc<AppState>, user_id: i64) -> Result<()> {
    let Some(mut online_user) = user_manager::get_user(state, user_id).await else {
        return Ok(());
    };

    let user_data = state
        .db
        .run_read_row(&format!(
            "SELECT name,figure,sex,mission FROM users WHERE id = '{}' LIMIT 1",
            user_id
        ))
        .await?;
    if user_data.len() < 4 {
        return Ok(());
    }

    let username = user_data[0].clone();
    let figure = user_data[1].clone();
    let sex = user_data[2].clone();
    let mission = user_data[3].clone();

    let _ = online_user.sender.send(format!(
        "@E{}{}{}{}{}{}{}{}H{}HH",
        user_id, '\u{2}', username, '\u{2}', figure, '\u{2}', sex, '\u{2}', mission
    ));

    online_user.figure = figure.clone();
    state
        .online_users
        .write()
        .await
        .insert(user_id, online_user.clone());

    if online_user.in_room
        && online_user.room_id > 0
        && let Some(mut room) = room_manager::get_room(state, online_user.room_id).await
        && let Some(index) = room.users.iter().position(|entry| entry.user_id == user_id)
    {
        room.users[index].figure = figure.clone();
        room.users[index].sex = sex.clone();
        room.users[index].mission = mission.clone();
        let room_uid = room.users[index].room_uid;
        let refresh_packet = format!(
            "DJ{}{}\u{2}{}\u{2}{}\u{2}",
            encode_vl64(room_uid as i32),
            room.users[index].figure,
            room.users[index].sex,
            room.users[index].mission
        );
        let recipients = room
            .users
            .iter()
            .map(|entry| entry.user_id)
            .collect::<Vec<_>>();
        room_manager::save_room(state, room).await;
        for target_user_id in recipients {
            if let Some(target_user) = user_manager::get_user(state, target_user_id).await {
                let _ = target_user.sender.send(refresh_packet.clone());
            }
        }
    }

    Ok(())
}

pub(crate) async fn mus_refresh_valueables(
    state: &Arc<AppState>,
    user_id: i64,
    credits: bool,
    tickets: bool,
) -> Result<()> {
    let Some(user) = user_manager::get_user(state, user_id).await else {
        return Ok(());
    };

    if credits {
        let credits = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT credits FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;
        let _ = user.sender.send(format!("@F{}", credits));
    }

    if tickets {
        let tickets = state
            .db
            .run_read_unsafe_i64(&format!(
                "SELECT tickets FROM users WHERE id = '{}' LIMIT 1",
                user_id
            ))
            .await;
        let _ = user.sender.send(format!("A|{}", tickets));
    }

    Ok(())
}

pub(crate) async fn mus_refresh_club(state: &Arc<AppState>, user_id: i64) -> Result<()> {
    let Some(user) = user_manager::get_user(state, user_id).await else {
        return Ok(());
    };

    let details = state
        .db
        .run_read_row(&format!(
            "SELECT months_expired,months_left,date_monthstarted FROM users_club WHERE userid = '{}' LIMIT 1",
            user_id
        ))
        .await?;

    let mut resting_days = 0;
    let mut passed_months = 0;
    let mut resting_months = 0;
    if details.len() >= 3 {
        passed_months = details[0].parse::<i32>().unwrap_or(0);
        resting_months = details[1].parse::<i32>().unwrap_or(0) - 1;
        resting_days = parse_club_days(&details[2]);
    }

    let _ = user.sender.send(format!(
        "@Gclub_habbo{}{}{}{}{}",
        '\u{2}',
        encode_vl64(resting_days),
        encode_vl64(passed_months),
        encode_vl64(resting_months),
        encode_vl64(1)
    ));
    Ok(())
}

pub(crate) async fn mus_refresh_badges(state: &Arc<AppState>, user_id: i64) -> Result<()> {
    let Some(user) = user_manager::get_user(state, user_id).await else {
        return Ok(());
    };

    let badges = state
        .db
        .run_read_column_string(&format!(
            "SELECT COALESCE(NULLIF(badge, ''), badgeid) FROM users_badges WHERE userid = '{}' ORDER BY slotid ASC",
            user_id
        ))
        .await?;
    let slot_ids = state
        .db
        .run_read_column_i64(&format!(
            "SELECT slotid FROM users_badges WHERE userid = '{}' ORDER BY slotid ASC",
            user_id
        ))
        .await?;

    let mut payload = encode_vl64(badges.len() as i32);
    for badge in &badges {
        payload.push_str(badge);
        payload.push('\u{2}');
    }

    for (index, badge) in badges.iter().enumerate() {
        let slot_id = *slot_ids.get(index).unwrap_or(&0);
        if slot_id > 0 {
            payload.push_str(&encode_vl64(slot_id as i32));
            payload.push_str(badge);
            payload.push('\u{2}');
        }
    }

    let current_badges = collect_current_badges(&badges, &slot_ids);
    persist_current_badges(state, user_id, &current_badges).await?;
    let _ = user.sender.send(format!("Ce{}", payload));
    let _ = user.sender.send(format!("Ft{}", BADGE_ACHIEVEMENTS));
    let _ = user.sender.send(format!("DtIH{}\u{2}FCH", '\u{1}'));
    Ok(())
}

pub(crate) async fn mus_refresh_hand(state: &Arc<AppState>, user_id: i64) {
    refresh_hand_for_user(state, user_id, "new").await;
}

pub(crate) async fn mus_kick_user_from_room(
    state: &Arc<AppState>,
    user_id: i64,
    message: &str,
) -> Result<()> {
    let Some(user) = user_manager::get_user(state, user_id).await else {
        return Ok(());
    };
    if !user.in_room || user.room_id <= 0 {
        return Ok(());
    }

    let mut room = room_manager::load_room(state, user.room_id, user.room_is_public).await?;
    let Some(target) = room.kick_user(user_id) else {
        return Ok(());
    };
    apply_room_kick_outcome_for_room(
        state,
        user.room_id,
        room,
        crate::virtuals::rooms::virtual_room::RoomKickOutcome {
            targets: vec![target],
        },
        message,
    )
    .await
}

async fn apply_room_kick_outcome_for_room(
    state: &Arc<AppState>,
    room_id: i64,
    room: crate::virtuals::rooms::virtual_room::VirtualRoom,
    outcome: crate::virtuals::rooms::virtual_room::RoomKickOutcome,
    message: &str,
) -> Result<()> {
    if outcome.targets.is_empty() {
        return Ok(());
    }

    let remaining_users = room
        .users
        .iter()
        .map(|entry| entry.user_id)
        .collect::<Vec<_>>();
    let visitor_count = room.users.len() as i64;
    if visitor_count > 0 {
        room_manager::update_room_visitor_count(state, room_id, visitor_count).await?;
        room_manager::save_room(state, room).await;
    } else {
        room_manager::remove_room(state, room_id).await?;
    }

    for target in &outcome.targets {
        for user_id in &remaining_users {
            if let Some(user) = user_manager::get_user(state, *user_id).await {
                let _ = user.sender.send(format!("@]{}", target.room_uid));
            }
        }
    }
    for target in outcome.targets {
        state.clear_doorbell_access(target.user_id).await;
        state.clear_doorbell_denied(target.user_id).await;
        if let Some(mut user) = user_manager::get_user(state, target.user_id).await {
            // Legacy Holograph cleared the removed user's live room flags as part of room
            // removal. Rust sessions keep some room fields locally, so we mirror the shared
            // online-user state here and let the owning session reconcile on follow-up packets.
            user.in_room = false;
            user.room_id = 0;
            user.room_is_public = false;
            state
                .online_users
                .write()
                .await
                .insert(target.user_id, user);
        }
        if let Some(user) = user_manager::get_user(state, target.user_id).await {
            let _ = user.sender.send("@R".to_string());
            if !message.is_empty() {
                let _ = user
                    .sender
                    .send(format!("B!{}\u{2}holo.cast.modkick", message));
            }
        }
    }
    Ok(())
}

fn parse_club_days(value: &str) -> i32 {
    let parsed = chrono::NaiveDateTime::parse_from_str(value, "%d/%m/%Y %H:%M:%S")
        .or_else(|_| chrono::NaiveDate::parse_from_str(value, "%d/%m/%Y").map(midnight))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map(midnight));

    match parsed {
        Ok(started) => (started - chrono::Local::now().naive_local()).num_days() as i32 + 32,
        Err(_) => 0,
    }
}

fn midnight(date: chrono::NaiveDate) -> chrono::NaiveDateTime {
    date.and_hms_opt(0, 0, 0).expect("valid midnight")
}

#[allow(dead_code)]
fn _decode_badge_name_length(data: &str) -> usize {
    decode_b64(&data[..2]).unwrap_or(0)
}
