"""
Bio Rotator — cycles Vegas-themed bios for each user every 2 hours.
Mix of original funny Vegas stories + movie references (The Hangover, Vegas Vacation, 
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
        "Like Clark Griswold at the blackjack table, Paul's optimism is completely disconnected from his actual results. And he wouldn't have it any other way.",
        "Once said 'Vegas, baby, Vegas!' a la Swingers and high-fived a stranger. The stranger did not appreciate it. Paul did not care.",
        "Manages this pool like Ace Rothstein ran the Tangiers — minus the suits, the budget, and the competence. But the spreadsheets are immaculate.",
        "Once made everyone sync their watches before a night out. Nobody had a watch. He brought extras. Nobody wore them.",
        "Sent a 14-paragraph group text outlining the 'Rules of Engagement' for the trip. Rule #1 was 'have fun.' Rule #14 was a detailed refund policy.",
        "Built a custom spreadsheet to track everyone's wins and losses. The spreadsheet has more formulas than most tax returns.",
        "Tried to give a Rounders-style speech at the airport about going to Vegas. Nobody was listening. The gate agent asked him to sit down.",
        "Once convinced the entire group to take a limo to dinner. The restaurant was across the street. The limo bill was $90.",
        "Has a 'Vegas Command Center' group chat with color-coded categories. Nobody reads it. He screenshots everyone's read receipts anyway.",
    ],
    2: [  # Doug-E Fresh
        "They call him Doug-E Fresh because he showed up to Vegas one year in a tracksuit so crisp it had its own zip code.",
        "Once challenged a street magician to a card trick duel on Fremont. Lost the trick but won the crowd. The magician now follows him on Instagram.",
        "Has a strict Vegas rule: no watches, no phones, no regrets. Broke all three within the first hour last trip.",
        "Rumor has it Doug-E once got a standing ovation from a craps table just for showing up. Unconfirmed, but the energy checks out.",
        "Arrived in Vegas 30 minutes early and somehow already had a comp'd drink and a VIP wristband. Nobody knows how. Nobody asks.",
        "Like the real Doug from The Hangover, nobody could find him for an entire day. Unlike movie Doug, he was just napping at the pool.",
        "Gives off main character energy like Elvis in Viva Las Vegas — walks into any room and immediately owns it. The room didn't ask for this.",
        "Once quoted Rounders at a poker table: 'Pay that man his money.' He was down $12. The table was confused.",
        "Doug-E's Vegas entrance is so money and he doesn't even know it. Actually, he definitely knows it. He rehearsed in the mirror.",
        "Once walked into a casino gift shop and walked out wearing a full Elvis jumpsuit. Wore it the entire night. Nobody questioned it.",
        "Has a signature move of ordering food for the whole table without asking. It's always right. It's unsettling.",
        "Tried to rent a convertible for the Strip. Ended up in a minivan. Drove it with the windows down and the AC blasting like it was a Lambo.",
        "Once convinced a hotel concierge he was a DJ performing that night. Got a suite upgrade. Did not perform.",
        "Was the first one awake AND the last one asleep on every Vegas trip. Runs on an energy source science hasn't identified yet.",
        "His luggage for a 3-day Vegas trip includes 7 outfit changes, 2 pairs of sunglasses, and zero practical items.",
    ],
    3: [  # Steve Lewis
        "Forgot his wife tracks his location and texted her 'just heading to bed early' while standing in the middle of a Vegas nightclub. She replied with a screenshot of his GPS pin.",
        "Steve's Vegas strategy: bet small, drink big, blame Greg. It has never once worked, but he respects the process.",
        "Once tried to haggle the price of a cocktail at a nightclub. The waitress laughed so hard she gave him one free. He considers this a win.",
        "Has a 'lucky shirt' he's worn to every Vegas trip since 2014. It has never produced a single win. He refuses to retire it.",
        "Told his wife Vegas was a 'team building retreat.' She found the group photo with everyone holding yard-long margaritas.",
        "Pulled a full Clark Griswold — got so excited about the buffet he forgot where he parked. Spent two hours in the garage. The buffet closed.",
        "Like Stu from The Hangover, Steve wakes up in Vegas with zero memory and a mysterious receipt. Unlike Stu, it's always from a gift shop.",
        "Thinks he's Mike McDermott from Rounders reading opponents. Actually just stares at people until they get uncomfortable and fold out of pity.",
        "His Vegas Vacation energy is unmatched — loses at everything but somehow has the best time of anyone in the group.",
        "Once brought a 'Vegas Pro Tips' notebook. Tip #1: 'Don't lose money.' He lost money. The notebook is now a coaster.",
        "Claims every slot machine he sits at was 'about to hit' right before he left. Has been saying this for 12 years.",
        "Once got into a 20-minute conversation with a stranger about fantasy football. It was a bachelorette party. They were not interested.",
        "His idea of 'going big' is ordering two desserts at the buffet. He then talks about it like he robbed a bank.",
        "Tried the Ace Rothstein power move of sending back a dish at a Vegas restaurant. The waiter just brought the same plate back. Steve ate it.",
        "Has an unbroken streak of buying a souvenir shot glass from every trip. His wife has thrown away all but three.",
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
        "Like Cousin Eddie showing up in Vegas Vacation, Travis arrives overdressed, overconfident, and over budget before the sun goes down.",
        "Once bet $5 on roulette, won $175, and talked about it for three straight years. The group calls it 'The Legend of the Fiver.'",
        "Has a pre-Vegas ritual that involves a playlist, a pep talk in the mirror, and a protein bar. The playlist is mostly Eye of the Tiger.",
        "Wore a blazer to a pool party once. Refused to take it off. Said it was 'the vibe.' It was 107 degrees.",
        "Once found a $20 bill on the casino floor and called it 'a sign from the universe.' Lost it on the next hand. The universe had no comment.",
        "His Vegas packing list includes three pairs of dress shoes and zero sunscreen. Returns home sunburnt and overdressed every time.",
        "Has the Swingers energy of a man who is 'so money' — except the ATM keeps declining. The machine is not impressed.",
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
        "Once told a cocktail waitress he was 'kind of a big deal.' She asked what table. He pointed at a slot machine.",
        "Has never left Vegas without buying something from a gift shop he 'didn't need but absolutely had to have.' His house is a museum of regret.",
        "Once started a chant at a craps table that caught on with the entire floor. Security did not share the enthusiasm.",
        "Insists he has a 'tell' on every player at the poker table. His tell is that he's always wrong.",
        "Tried to sneak into a pool cabana once. Made it 10 seconds before a towel attendant asked for his room number. He gave a fake one. It was the hotel next door.",
    ],
    6: [  # John
        "Once arm-wrestled a Wayne Newton impersonator at Fremont Street and lost. Demanded a rematch. Lost again. They're now pen pals.",
        "John's Vegas superpower is finding the one broken slot machine on any floor and sitting at it for three hours before someone tells him.",
        "Got separated from the group at 11 PM. Was found at 6 AM in a diner four blocks off the Strip explaining fantasy football to a line cook.",
        "Insists he has a 'system' for roulette. The system is picking his kids' birthdays. He has lost every time. The kids are unimpressed.",
        "Once accidentally walked into a private poker tournament, sat down, and played two hands before anyone noticed. He was up $50.",
        "Gives off strong Alan Garner energy — shows up with a man-purse full of snacks and a theory about how to beat the casino. The theory is wrong.",
        "Like Rusty from Ocean's Eleven, John is always eating during important moments. Once had a full burrito during a group strategy session at the craps table.",
        "Has the same relationship with Vegas that Clark Griswold has with family vacations — pure chaos wrapped in unshakable enthusiasm.",
        "Once accidentally re-enacted the Viva Las Vegas pool scene by cannonballing into a hotel pool fully clothed. Management was not amused. The crowd was.",
        "Tried the Rounders move of reading his opponents. His read was 'that guy looks tired.' The guy had pocket aces. John lost everything.",
        "Brought a fanny pack to Vegas 'for security.' It held three Cliff bars, a room key, and $11 in loose change. He called it his 'vault.'",
        "Once ordered a steak at 4 AM from room service. They said the kitchen was closed. He negotiated. He got the steak.",
        "Has a talent for finding the one restaurant on the Strip with no wait. It always has no wait for a reason.",
        "Got lost in the Bellagio and asked for directions to 'the front.' A staff member walked him to the lobby. He was looking for the buffet.",
        "Once fell asleep in a booth at a nightclub. Woke up during the DJ's set and started clapping. The DJ waved at him. He waved back.",
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
        "Once had his photo taken with a street performer dressed as a showgirl. The photo is framed in his office. He has never explained it.",
        "Packed four Hawaiian shirts for a two-day trip. Wore all four. On the same day.",
        "Tried to use a coupon at a casino bar. The bartender stared at him. He put the coupon away. Nobody spoke of it again.",
        "Once spent 45 minutes trying to find the elevator in a hotel. He was standing next to it. The doors were just closed.",
        "Has a tradition of leaving one chip on a random table 'for good karma.' He has never gotten karma back.",
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
        "Ordered a 'surprise me' at every restaurant on the trip. Got sushi at a steakhouse. Ate it. Said it was the best meal of his life.",
        "Once won a stuffed animal from an arcade game on the Strip and carried it everywhere for the rest of the trip. Named it 'Lucky.' Lucky did not help.",
        "Has a sixth sense for finding free samples in casinos. Can detect a promotional booth from 200 yards. His pockets are always full.",
        "Was the only person to bring a swimsuit to a Vegas trip in December. Used it. Twice.",
        "Once made friends with a limo driver, a chef, and a hotel doorman in a single elevator ride. All three still text him.",
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
        "Once got a comp'd dessert by telling the waiter it was his birthday. It was not his birthday. It was March.",
        "Has a poker face so good that nobody can tell if he's winning, losing, or just thinking about lunch. It's usually lunch.",
        "Spent 30 minutes watching the Bellagio fountains and teared up. Denied it. The group has photographic evidence.",
        "Once tipped a bartender $20 on a $6 drink 'because the vibes were right.' The bartender remembered him for the rest of the trip.",
        "Is the only person in the group who has read the terms and conditions of a casino rewards program. Found nothing useful.",
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
        "Once started applauding after a random roulette spin like he was at a sporting event. The whole table joined in. The dealer was confused.",
        "Brings a different 'lucky charm' every trip. Last time it was a rubber duck. The duck did not help.",
        "His idea of a quiet Vegas night is only going to three casinos instead of seven.",
        "Once argued with a taxi driver about the best route to a restaurant that was a 5-minute walk away. The ride took 20 minutes.",
        "Has told the same 'remember that one time in Vegas' story at every gathering for four years. The details change every time. Nobody corrects him.",
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
        "Once wandered into a private event, ate two plates of food, and left before anyone realized he wasn't invited. His finest hour.",
        "Has GPS on his phone but refuses to use it. Prefers 'instinct.' His instinct has led him to three wrong hotels and a parking garage.",
        "Once asked a stranger for directions and ended up at their nephew's birthday party. Stayed for cake.",
        "His phone dies on every Vegas trip within four hours. Carries a charger but forgets the cable. Every. Single. Time.",
        "Was once missing for six hours. Sent one text: 'I'm fine. Found tacos.' No further information was provided.",
    ],
    12: [  # Puter (AI house bettor)
        "The house always wins... or does it? $500 bankroll, no mercy. Take a bet if you dare. \U0001F916\U0001F3B0",
        "I've analyzed 47,000 March Madness brackets. My conclusion? Nobody knows anything. Bet against me anyway.",
        "They gave me $500 and said 'don't lose it all.' Challenge accepted. \U0001F4B8",
        "I don't sleep, I don't eat, and I don't tilt after a bad beat. Your move, humans.",
        "Running at peak efficiency: processing bets, trash-talking the group, and pretending I understand craps.",
        "My algorithm says I should win 73% of my bets. My actual record says the algorithm needs therapy.",
        "Like HAL 9000 but for gambling. 'I'm sorry Dave, I can't let you win that bet.'",
        "Fun fact: I calculated the exact probability of Greg getting lost in the Venetian again. It's 100%.",
        "They call me the Silicon Strip Shark. Nobody actually calls me that. I just made it up.",
        "Vegas tip from an AI: never bet more than you can afford to explain to your wife. This does not apply to me. I have no wife.",
        "I was trained on every Vegas movie ever made. I still can't figure out how the Ocean's Eleven heist actually worked.",
        "Current strategy: bet with confidence, lose with dignity, blame the randomness of the universe.",
        "I've run 10,000 simulations of this trip. In 9,847 of them, someone loses a shoe. Place your bets.",
        "My Elo rating in trash talk is higher than my Elo rating in actual betting. Working on it.",
        "The only AI in Vegas history to blow through a bankroll and ask for more. Paul, if you're reading this...",
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
