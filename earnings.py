from datetime import datetime, timedelta
import re

date_to_company = {
    datetime(2023, 9, 13): "Frontier Developments",
    datetime(2023, 9, 16): "Gamestop",
    datetime(2023, 9, 19): "Team17",
    datetime(2023, 9, 21): "People Can Fly",
    datetime(2023, 9, 25): "Devolver Digital",
    datetime(2023, 9, 26): "tinyBuild",
    datetime(2023, 9, 27): "Digital Bros/505 Games",
    datetime(2023, 9, 29): "CI Games",
    datetime(2023, 10, 16): "DON'T NOD",
    datetime(2023, 10, 18): "Netflix",
    datetime(2023, 10, 19): "Focus Entertainment",
    datetime(2023, 10, 24): "Microsoft",
    datetime(2023, 10, 26): ["Capcom", "Ubisoft", "Paradox Interactive"],
    datetime(2023, 10, 30): ["NACON", "KOEI Tecmo"],
    datetime(2023, 10, 31): "Remedy",
    datetime(2023, 11, 1): "EA",
    datetime(2023, 11, 2): ["Kadokawa", "Paramount", "Konami"],
    datetime(2023, 11, 7): ["Bandai Namco", "Nintendo"],
    datetime(2023, 11, 8): ["Take-Two*", "SEGA Sammy", "Disney", "Roblox", "Warner Discovery"],
    datetime(2023, 11, 9): ["NEXON", "Square Enix*", "Krafton*", "Sony"],
    datetime(2023, 11, 14): "Bloober Team*",
    datetime(2023, 11, 15): ["Tencent", "NetEase*", "Maximum Entertainment", "Thunderful Group"],
    datetime(2023, 11, 16): ["Embracer Group", "Starbreeze"],
    datetime(2023, 11, 23): "11bit Studios",
    datetime(2023, 11, 28): "CD Projekt Group",
}

def list_to_sentence(lst):
    if len(lst) == 1:
        return f"**{lst[0]}**"
    elif len(lst) == 2:
        return f"**{lst[0]}** and **{lst[1]}**"
    else:
        return ', '.join([f"**{company}**" for company in lst[:-1]]) + f", and **{lst[-1]}**"

async def earnings_command(ctx, *args):
    valid_formats = [
        'YYYY-MM-DD',
        'MM/DD/YYYY',
        'YYYY/MM/DD',
        'YYYYMM (for a whole month)',
    ]
    if args:
        date_str = args[0]
        # YYYY-MM-DD format
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y-%m-%d')
        # MM/DD/YYYY format
        elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
            query_date = datetime.strptime(date_str, '%m/%d/%Y')
        # YYYY/MM/DD format
        elif re.match(r'\d{4}/\d{2}/\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y/%m/%d')
        # YYYYMM format for a whole month
        elif re.match(r'\d{4}\d{2}', date_str):
            month_companies = {date: company for date, company in date_to_company.items() if date.strftime('%Y%m') == date_str}
            if month_companies:
                companies = ', '.join(month_companies.values())
                await ctx.send(f"{date.strftime('%Y-%m-%d')}: {companies}")
                return
            else:
                await ctx.send('No earnings in this month.')
                return
        else:
            await ctx.send(f'Invalid date format. Valid formats are: {", ".join(valid_formats)}.')
            return

        earnings_info = date_to_company.get(query_date, 'No earnings on this date.')
        await ctx.send(earnings_info)

    else:
        # No argument, show next week's companies including today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Set the time to 00:00:00
        next_week = today + timedelta(days=7)
        next_week_companies = {date: company for date, company in date_to_company.items() if today <= date <= next_week}

        if next_week_companies:
            sentences = []
            for date, companies in next_week_companies.items():
                if isinstance(companies, list):
                    companies_str = list_to_sentence(companies)
                else:
                    companies_str = f"**{companies}**"
                    
                sentences.append(f"{date.strftime('%A, %Y-%m-%d')}: {companies_str}")

            final_output = 'Here are the gaming companeis with earnings for the next 7 days:\n' + '\n'.join(sentences)
            await ctx.send(final_output)
        else:
            await ctx.send('No earnings in the next 7 days.')
