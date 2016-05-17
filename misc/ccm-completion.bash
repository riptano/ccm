##############################################################################
# ccm-completion.bash
#
# CCM command completion logic for bash. It dynamically determines available
# sub-commands based on the ccm being invoked. Thus, users running multiple
# ccm's (or a ccm that they are continuously updating with new commands) will
# automagically work.
#
# This completion script relies on ccm having two hidden subcommands:
# show-cluster-cmds - emits the names of cluster sub-commands.
# show-node-cmds - emits the names of node sub-commands.
#
# Usage:
# * Copy the script into your home directory or some such.
# * Source it from your ~/.bash_profile: . ~/scripts/ccm-completion.bash
##############################################################################

case "$COMP_WORDBREAKS" in
*:*) : great ;;
*)   COMP_WORDBREAKS="$COMP_WORDBREAKS:"
esac

__ccmcomp_is_node ()
{
    local CANDIDATE=$1
    
    # Get the list of nodes from 'ccm list' and then see if the given node 
    # name is in it.
    local RC=1
    for i in $(__ccmcomp_node_list) ; do
        if [ "$CANDIDATE" == "$i" ] ; then
            RC=0
            break
        fi
    done
    
    return $RC
}

__ccmcomp_node_list ()
{
    ${COMP_WORDS[0]} status | grep -E ': (DOWN|UP)' | sed -e 's/:.*//'
}

__ccm_switch ()
{
    local CMD=${COMP_WORDS[0]}
    local WORD=$1
    COMPREPLY=( $(compgen -W "$($CMD list | sed -E -e 's/ *\*?//')" $WORD) )
}

__ccmcomp_cluster_cmd_filter ()
{
    local CMD=${COMP_WORDS[0]}
    local WORD=$2
    local PREV_WORD=$3
    # Is this the first arg to ccm? If so, we want cluster commands.
    if [ $COMP_CWORD == 1 ] ; then
        # We get the cluster-cmds via a background process to make this go a
        # bit faster. 10s-100s of milliseconds make a difference here because
        # a user is waiting on the completion response.
        exec 3< <($CMD show-cluster-cmds)
        COMPREPLY=( $(compgen -W "$(__ccmcomp_node_list) $(cat <&3)" $WORD) )
    else
        # PREV_WORD is a sub-command or node name. If it's a node name,
        # show (filtered) node commands.
        if __ccmcomp_is_node $PREV_WORD ; then
            COMPREPLY=( $(compgen -W "$($CMD show-node-cmds)" $WORD) )
        else
            # It's a subcommand. Call the argument filter function for that
            # sub-command if it exists
            if type __ccm_$PREV_WORD > /dev/null 2>&1 ; then
                __ccm_$PREV_WORD $WORD
            else
                return 1
            fi
        fi
    fi
}

# Bind completions for ccm to invoke our top-level handling function.
complete -F __ccmcomp_cluster_cmd_filter ccm
