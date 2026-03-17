"""
Bio Rotator — cycles Vegas-themed bios for each user every 2 hours.
Mix of original Vegas humor + movie references (The Hangover, Vegas Vacation, 
Rounders, Viva Las Vegas, Ocean's Eleven, Casino, 21, Swingers).
"""

ROTATING_BIOS = {
    1: [  # Paul
        "The man behind the madness. Last time in Vegas he tried to bet on himself in a pool and got shut down by his own rules.",
        "Self-appointed Commissioner of Fun. Once filibustered a dinner reservation for 40 minutes arguing why In-N-Out counts as fine dining on the Strip.",
        "Runs this pool like a Vegas pit boss — zero tolerance, maximum drama. Has been seen whispering to spreadsheets at 2 AM.",
        "Claims he came to Vegas for the culture. His browser history says otherwise. Still hasn't explained the $47 room service charge for 'just water.'",
        "The Vegas Trip Architect. His group texts start with 'Alright boys, listen up' and end with someone getting a new nickname they didn't ask for.",
        "Thinks he's Danny Ocean running a master plan. In reality he's more like Rusty — always eating and talking at the same time.",
        "Tried to give a Rounders-style 'I'm going to Vegas' speech at the airport. Nobody was listening. The gate agent asked him to sit down.",
        "Like Clark Griswold at the blackjack table, Paul's optimism is completely disconnected from his actual results. And he wouldn't have it any other way.",
        "Once said 'Vegas, baby, Vegas!' à la Swingers and high-fived a stranger. The stranger did not appreciate it. Paul did not care.",
        "Manages this pool like Ace Rothstein ran the Tangiers — minus the suits, the budget, and the competence. But the spreadsheets are immaculate.",
    ],
    2: [  # Doug-E Fresh
        "They call him Doug-E Fresh because he showed up to Vegas one year in a tracksuit so crisp it had its own zip code.",
        "Once challenged a street magician to a card trick duel on Fremont. Lost the trick but won the crowd. The magician now follows him on Instagram.",
        "Has a strict Vegas rule: no watches, no phones, no regrets. Broke all three within the first hour last trip.",
        "Rumor has it Doug-E once got a standing ovation from a craps table just for showing up. Unconfirmed, but the energy checks out.",
        "Arrived in Vegas 30 minutes early and somehow already had a comp'd drink and a VIP wristband. Nobody knows how. Nobody asks.",
        "Like the real Doug from The Hangover, nobody could find him for an entire day. Unlike movie Doug, he was just napping at the pool.",
        "Gives off main character energy like Elvis in Viva Las Vegas — walks into any room and immediately owns it. The room didn't ask for this.",
        "Swears he has Ocean's Eleven-level heist energy. In practice, he once spent 20 minutes trying to figure out how the hotel key card works.",
        "Doug-E's Vegas entrance is so money and he doesn't even know it. Actually, he definitely knows it. He rehearsed in the mirror.",
        "Once quoted Rounders at a poker table: 'Pay that man his money.' He was down $12. The table was confused.",
    ],
    3: [  # Steve Lewis
        "Forgot his wife tracks his location and texted her 'just heading to bed early' while standing in the middle of a Vegas nightclub. She replied with a screenshot of his GPS pin.",
        "Steve's Vegas strategy: bet small, drink big, blame Greg. It has never once worked, but he respects the process.",
        "Once tried to haggle the price of a cocktail at a nightclub. The waitress laughed so hard she gave him one free. He considers this a win.",
        "Has a 'lucky shirt' he's worn to every Vegas trip since 2014. It has never produced a single win. He refuses to retire it.",
        "Told his wife Vegas was a 'team building retreat.' She found the group photo with everyone holding yard-long margaritas.",
        "Pulled a full Clark Griswold — got so excited about the buffet he forgot where he parked. Spent two hours in the garage. The buffet closed.",
        "Like Stu from The Hangover, Steve wakes up in Vegas with zero memory and a mysterious receipt. Unlike Stu, it's always from a gift shop.",
        "Tried the Ace Rothstein power move of sending back a dish at a Vegas restaurant. The waiter just brought the same plate back. Steve ate it.",
        "Thinks he's Mike McDermott from Rounders reading opponents. Actually just stares at people until they get uncomfortable and fold out of pity.",
        "His Vegas Vacation energy is unmatched — loses at everything but somehow has the best time of anyone in the group.",
    ],
    4: [  # Travis Sjostrom
        "Legend has it Travis once tipped a blackjack dealer with a half-eaten sandwich and a firm handshake. The dealer said it was the best tip all night.",
        "Travis doesn't gamble — he 'invests aggressively in short-term entertainment.' His portfolio is down 100% every trip.",
        "Was once mistaken for a high roller at the Wynn because he walked in with unearned confidence and a sport coat. Got a free steak before they caught on.",
        "Holds the group record for most buffet plates in one sitting. Won't reveal the number but the staff started clapping.",
        "Travis's Vegas motto: 'You can't lose if you don't check your bank account.' He has not checked since 2022.",
        "Walks into casinos with full Danny Ocean confidence. Walks out with full Clark Griswold results.",
        "Once did the Hangover-style rooftop toast with the boys. Spilled his drink immediately. The toast was about 'not wasting this trip.'",
        "Channels his inner Teddy KGB at poker night: intense stare, dramatic chip shuffling. Still can't tell a flush from a straight.",
        "Has the Swingers energy of a man who is 'so money' — except the ATM keeps declining. The machine is not impressed.",
        "Like Cousin Eddie showing up in Vegas Vacation, Travis arrives overdressed, overconfident, and over budget before the sun goes down.",
    ],
    5: [  # Chris (A)
        "Claims he once won $10,000 on a single roulette spin, bought everyone drinks, then lost $10,000 on the next spin. Nobody can confirm because 'what happens in Vegas.'",
        "Chris has a signature Vegas move: ordering bottle service, realizing the price, then quietly switching to beer and pretending it was the plan all along.",
        "Once got into a heated argument with a slot machine. Witnesses say he called it 'disrespectful.' The machine did not respond.",
        "Has a theory that if you walk fast enough through a casino, the losses don't count. Physics disagrees. His wallet agrees.",
        "Tried counting cards once. Lost count at 7. Switched to blackjack 'vibes-based strategy.' Down $400 in twenty minutes.",
        "Fancies himself the Ben Campbell of the group — MIT card-counting genius from 21. In reality, he mouths the numbers out loud and the pit boss just watches.",
        "Like Phil from The Hangover, Chris is the one who says 'this is going to be the best night ever' right before everything goes sideways.",
        "Tried the Ocean's Eleven 'act like you belong' move in a VIP section. Lasted 90 seconds before a bouncer politely ended the performance.",
        "Has Nicky Santoro energy from Casino — small but scrappy. Once argued with a valet for ten minutes over a $5 tip. The valet won.",
        "Quotes Swingers constantly. 'You're so money.' 'Vegas, baby.' The group has a running count. Last trip it hit 47.",
    ],
    6: [  # John
        "Once arm-wrestled a Wayne Newton impersonator at Fremont Street and lost. Demanded a rematch. Lost again. They're now pen pals.",
        "John's Vegas superpower is finding the one broken slot machine on any floor and sitting at it for three hours before someone tells him.",
        "Got separated from the group at 11 PM. Was found at 6 AM in a diner four blocks off the Strip explaining fantasy football to a line cook.",
        "Insists he has a 'system' for roulette. The system is picking his kids' birthdays. He has lost every time. The kids are unimpressed.",
        "Once accidentally walked into a private poker tournament, sat down, and played two hands before anyone noticed. He was up $50.",
        "Gives off strong Alan Garner energy — shows up with a man-purse full of snacks and a theory about how to beat the casino. The theory is wrong.",
        "Like Rusty from Ocean's Eleven, John is always eating during important moments. Once had a full burrito during a group strategy session at the craps table.",
        "Tried the Rounders move of reading his opponents. His read was 'that guy looks tired.' The guy had pocket aces. John lost everything.",
        "Has the same relationship with Vegas that Clark Griswold has with family vacations — pure chaos wrapped in unshakable enthusiasm.",
        "Once accidentally re-enacted the Viva Las Vegas pool scene by cannonballing into a hotel pool fully clothed. Management was not amused. The crowd was.",
    ],
    7: [  # Chris Dieringer
        "Was mistaken for a Cirque du Soleil performer at the Bellagio and went along with it for 45 minutes before security escorted him out of the fountain.",
        "Chris D. once tried to check into the wrong hotel, argued with the front desk for 20 minutes, then realized he was at a spa.",
        "Has been kicked out of exactly one Vegas establishment. Won't say which. Everyone else remembers.",
        "Brought a 'Vegas Budget' spreadsheet. Blew through it by dinner on Day 1. The spreadsheet has been in therapy since.",
        "Once wore sunglasses indoors at a poker table to look intimidating. Walked into a pillar on the way out. The table still talks about it.",
        "Full Casino Rothstein energy — showed up to Vegas in a three-piece suit once. It was 112 degrees. He did not last.",
        "Like Doug in The Hangover, Chris D. disappeared for an entire afternoon. Found on a lounge chair on the wrong hotel's roof. 'I was finding myself.'",
        "Tried to do the Swingers swing dance scene at a Vegas lounge. Cleared the floor. Not in the good way.",
        "Approaches poker like Worm from Rounders — all confidence, questionable fundamentals. Once raised pre-flop without looking at his cards 'for the vibe.'",
        "His Vegas Vacation moment: tried to win a car at a casino contest. Won a keychain. Displayed it proudly for the rest of the trip.",
    ],
    8: [  # Aaron
        "Once got banned from a Vegas buffet for 'competitive eating without a license.' Still maintains he was just hungry.",
        "Aaron approaches Vegas the way a golden retriever approaches a dog park — full speed, no plan, maximum joy.",
        "Tried to tip a street performer with a casino chip. The performer tried to cash it. Both learned something that day.",
        "Has a talent for finding the one pool chair with a broken leg at every hotel. Sits in it anyway. Calls it 'character.'",
        "Once fell asleep at a blackjack table mid-hand. The dealer played his hand for him. He won. Has been chasing that energy ever since.",
        "Like Alan from The Hangover, Aaron once tried to count cards using a system he invented in the shower. The system was just counting to ten.",
        "Has the energy of Cousin Eddie arriving in Vegas — immediately finds the cheapest buffet, the loosest slots, and the loudest shirt in the gift shop.",
        "Tried the Ocean's Eleven cool walk through a casino lobby. Tripped on a carpet seam. Got up and kept walking like nothing happened. Respect.",
        "Channels Viva Las Vegas Elvis energy at karaoke. The voice says no, but the hip swivel says 'I'm not leaving this stage.'",
        "Once quoted Casino at a roulette table: 'When you love someone, you've gotta trust them.' He was talking to the roulette wheel. He lost.",
    ],
    9: [  # Phil
        "Accidentally checked into a wedding chapel instead of his hotel. By the time he realized, he'd already RSVP'd to three receptions and caught a bouquet.",
        "Phil's Vegas persona is 'quiet confidence.' In practice, this means sitting silently at a poker table while slowly losing everything.",
        "Once spent an entire afternoon at a penny slot machine and walked away up $3.47. Called it 'the best ROI of the trip.'",
        "Has a tradition of buying a souvenir cowboy hat on every Vegas trip. Owns eleven. His wife has hidden nine.",
        "Was the last one to leave the casino floor every single night last trip. Not because he was winning — he just couldn't find the exit.",
        "Pulls a full Stu Price from The Hangover every trip — starts cautious, ends up with a story nobody believes and a receipt he can't explain.",
        "Has Lester from Casino energy — always in the background, quietly observing everything, remembering everyone's worst moments for future roasts.",
        "Like the guys in Swingers, Phil insists on calling every casino 'beautiful, baby.' The casinos have not responded to any of his compliments.",
        "Tried the Rounders 'splash the pot' move at a poker game. Dealer told him that's not a real thing outside the movies. Phil disagrees to this day.",
        "Gives off Elvis in Viva Las Vegas vibes — smooth, unbothered, and somehow always near a woman who's confused about how he got there.",
    ],
    10: [  # Christian
        "Holds the unofficial record for most consecutive hours spent at a craps table without understanding a single rule. Just keeps yelling 'shooter' at random.",
        "Christian once ordered a 'surprise me' cocktail at every bar on Fremont Street. The last bartender surprised him with water and a cab number.",
        "Brings the same pair of 'lucky socks' to every Vegas trip. They have not been lucky. They have also not been washed.",
        "Was dared to sing karaoke on the Strip. Did 'My Way' by Sinatra. Got a standing ovation from two tourists and a pigeon.",
        "Tried to organize a group photo in front of the Bellagio fountains. It took 45 minutes, three countdowns, and someone still blinked.",
        "Full Hangover energy — once woke up in Vegas wearing someone else's jacket with a room key to a hotel he didn't book. Still doesn't know whose jacket it is.",
        "Like Clark Griswold winning at blackjack, Christian has one legendary Vegas win he brings up constantly. It was $23. In 2018.",
        "Approaches every casino like Danny Ocean walking into the Bellagio. Leaves every casino like Clark Griswold leaving the blackjack table.",
        "Once tried to do the Rounders stare-down at a poker table. His opponent asked if he was okay. He was not.",
        "Has the same energy as Elvis showing up to perform in Viva Las Vegas — walks in like he owns the place, performs badly, and everyone still loves him.",
    ],
    11: [  # Greg
        "Got lost inside the Venetian for 3 days in 2019. Survived entirely on free cocktails and pretzel bites. Was found asleep in a gondola.",
        "Greg's sense of direction in Vegas is legendary — legendarily bad. Has used 'I'm exploring' as an excuse every trip since 2017.",
        "Once tried to swim in the Bellagio fountain on a dare. Made it exactly two steps before security appeared. Claims he 'chose' to stop.",
        "Has accidentally gambled at a table meant for a private event twice. Both times he was winning before they removed him.",
        "Greg's official role in the group is 'the one we have to go find.' He has earned this title every single trip without fail.",
        "Is basically Doug from The Hangover — every trip, someone has to organize a search party. Last time he was three floors up at the wrong pool.",
        "Has Cousin Eddie's gift for making himself at home anywhere. Once set up a full camp at a hotel pool that wasn't his. Staff let it go for four hours.",
        "Tried the Ocean's Eleven 'blend in with the staff' move. Put on a vest he found. Was immediately asked to bus a table. He did it.",
        "Like the guys in Swingers, Greg insists the night is 'still young' at 3 AM. The night disagrees. Greg's Uber rating also disagrees.",
        "Channels full Casino energy — not the cool Ace Rothstein part, more the 'how did this guy get in here and why won't he leave' part.",
    ],
}

def get_bio_index():
    """Calculate which bio index to use based on current time.
    Cycles every 2 hours starting from a fixed point."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    hours_since_epoch = int(now.timestamp()) // 3600
    return (hours_since_epoch // 2)

def rotate_bios():
    import psycopg2
    conn = psycopg2.connect('postgresql://neondb_owner:npg_bkNlfWGCVD95@ep-dawn-lake-am5wefih-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require')
    cur = conn.cursor()
    
    idx = get_bio_index()
    total_bios = len(list(ROTATING_BIOS.values())[0])
    updated = []
    
    for user_id, bios in ROTATING_BIOS.items():
        bio = bios[idx % len(bios)]
        cur.execute("UPDATE users SET bio = %s WHERE id = %s", (bio, user_id))
        cur.execute("SELECT display_name FROM users WHERE id = %s", (user_id,))
        name = cur.fetchone()[0]
        updated.append(f"  {name}: bio #{(idx % len(bios)) + 1}")
    
    conn.commit()
    conn.close()
    return idx % total_bios + 1, updated

if __name__ == "__main__":
    cycle, updated = rotate_bios()
    print(f"Bio rotation complete — cycle #{cycle}")
    for u in updated:
        print(u)
