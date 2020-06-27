import discord
from discord.ext.commands import HelpCommand 
import time 
from . import timezone
from datetime import datetime


def case(str_input, first_cap): 
    if isinstance(str_input, str) and isinstance(first_cap, bool):
        case_change = ("Admin", "Dev", "Joke", "Command")
        str_input = str_input.lower()
        for item in case_change:
            if item.lower() in str_input: 
                str_input = str_input.replace(item.lower(), item)
        
        if not first_cap: 
            str_input = str_input[:1].lower() + str_input[1:]
        
        return str_input
    else: 
        print("Please input a string and a boolean.")


def snake_case(str_input, to_snake): 
    div = ("admin", "dev", "joke")
    str_input = str_input.lower() 

    if isinstance(str_input, str) and isinstance(to_snake, bool) and to_snake: 
        for item in div: 
            if item in str_input:
                str_input = str_input.replace(item, item + "_")
        return str_input
    elif isinstance(str_input, str) and isinstance(to_snake, bool) and not to_snake and '_' in str_input:
        return str_input.replace('_', '')

    else: 
        print("Please input a string and two booleans")


def to_bool(arg):
    try: 
        if isinstance(arg, str): 
            arg.lower()

        evalBool = {}
        evalBool.update(dict.fromkeys(['true', 'y', 't', 1], True))
        evalBool.update(dict.fromkeys(['false', 'n', 'f', 0], False))
        
        return evalBool[arg]
    except: 
        return f"{arg} is not a boolean argument. Please input a boolean argument."


def get_time(time_zone, military_time, return_time): 

    military_time = to_bool(military_time)
    if isinstance(military_time, str): 
        military_time = False

    if not isinstance(time_zone, timezone.TimeZone):
        time_zone = timezone.TimeZone(time_zone)
    local_time = time.gmtime(time.time())

    offset = time_zone.get_offset()
    hour = local_time[3]
    minute = local_time[4]

    hour += offset[0]
    minute += offset[1]

    if minute > 60:
        minute -= 60
        hour += 1
    elif minute < 0:
        minute += 60
        hour -= 1

    if hour > 24:
        hour -= 24

    elif hour < 0:
        hour += 24

    if not military_time and hour > 12:
        hour -= 12

    time_list = [hour, minute, local_time[5]]

    return time_list


class EastHelpCommand(discord.ext.commands.HelpCommand):
    cog_list = ["Commands", "DevCommands", "AdminCommands", "JokeCommands"]

    def get_destination(self):
        # TODO: Make DMs a valid option
        ctx = self.context 
        return ctx.channel

    async def server_prefix(self):
        return (await self.context.bot.get_prefix(self.context))[0]
    
    async def gen_command_signature(self, command): 
        parent_command = command.full_parent_name 
        if parent_command: 
            return f"{await self.server_prefix()}{parent_command} {command.name} {command.signature}"
        else: 
            return f"{await self.server_prefix()}{command.name} {command.signature}"

    async def command_help(self, command, embed, signature = False): 
        if signature:
            embed.add_field(name = "Signature", value = await self.gen_command_signature(command))
            embed.add_field(name = "Documentation", value = command.help, inline = False)
        else: 
            embed.add_field(name = command.name, value = command.description, inline = False)
    
    async def send_bot_help(self, mapping):
        embed = discord.Embed(title = "Help", color = 0xff0000)
        for cog in mapping: 
            if cog is None: 
                continue 

            if cog.qualified_name == "DevCommands": 
                continue 
            
            embed.add_field(name = cog.qualified_name, value = cog.description, inline = False)
        await self.get_destination().send(embed = embed)

    async def send_cog_help(self, cog): 
        embed = discord.Embed(title = "Help", color = 0xff0000)
        embed.description = cog.description
        for command in cog.get_commands(): 
            await self.command_help(command, embed)
        await self.get_destination().send(embed = embed)

    async def send_group_help(self, group): 
        embed = discord.Embed(title = "Help", color = 0xff0000)
        embed.description = group.description
        await self.command_help(group, embed, True)
        content = ""
        for command in group.commands: 
            content += f"{command.name}: {command.description}\n"

        embed.add_field(name = "Subcommands", value = content)
        await self.get_destination().send(embed = embed)

    async def send_command_help(self, command):
        embed = discord.Embed(title = "Help", color = 0xff0000)
        await self.command_help(command, embed, True)
        await self.get_destination().send(embed = embed)

    
def date_format(date_format, date_str):
    format_list = []
    date_list = []
    for c in date_format:
        format_list.append(c)
        if c == "Y":
            format_list.append(c)
    for c in date_str:
        date_list.append(c)

    split_index = []
    for index, c in enumerate(format_list):
        if c != 'M' and c != 'D' and c != 'Y':
            split_index.append(index)

    i = 0
    for index in split_index:
        try:
            int(date_list[index])
            date_list.insert(i, '0')
        except:
            pass
        finally:
            i = index + 1
            if format_list[index - 1] == 'M':
                month = int(''.join(date_list[index - 2:index]))
            elif format_list[index - 1] == 'D':
                day = int(''.join(date_list[index - 2:index]))
            elif format_list[index -1] == 'Y':
                year = int(''.join(date_list[index - 4:index]))

            if format_list[-1] == 'M':
                month = int(''.join(date_list[-2:]))
            elif format_list[-1] == 'D':
                day = int(''.join(date_list[-2:]))
            elif format_list[-1] == 'Y':
                year = int(''.join(date_list[-4:]))

    print(f"{month}")
    date = datetime(year=year, month=month, day=day)
    return date
